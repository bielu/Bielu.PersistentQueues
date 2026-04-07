using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Bielu.PersistentQueues.Storage;

namespace Bielu.PersistentQueues.Partitioning;

/// <summary>
/// A queue implementation that supports Kafka-like partitioning on top of the existing <see cref="IQueue"/>.
/// </summary>
/// <remarks>
/// <para>
/// <see cref="PartitionedQueue"/> decorates an existing <see cref="IQueue"/> instance and maps
/// logical partitions to physical queues using the naming convention
/// <c>{queueName}:partition-{index}</c>.
/// </para>
/// <para>
/// Messages are routed to partitions via a configurable <see cref="IPartitionStrategy"/>.
/// When a message has a <see cref="Message.PartitionKey"/>, the strategy uses it to
/// consistently route related messages to the same partition. This guarantees ordering
/// within a partition key, similar to Kafka's partitioning model.
/// </para>
/// <para>
/// All non-partition-aware operations from <see cref="IQueue"/> are delegated to the inner queue.
/// </para>
/// </remarks>
public class PartitionedQueue : IPartitionedQueue
{
    private readonly IQueue _innerQueue;
    private readonly ConcurrentDictionary<string, int> _partitionCounts = new();
    private readonly ConcurrentDictionary<string, SemaphoreSlim> _partitionLocks = new();

    /// <summary>
    /// Initializes a new instance of the <see cref="PartitionedQueue"/> class.
    /// </summary>
    /// <param name="innerQueue">The underlying queue to delegate operations to.</param>
    /// <param name="partitionStrategy">The strategy used to assign messages to partitions.</param>
    /// <exception cref="ArgumentNullException">Thrown when innerQueue or partitionStrategy is null.</exception>
    public PartitionedQueue(IQueue innerQueue, IPartitionStrategy partitionStrategy)
    {
        _innerQueue = innerQueue ?? throw new ArgumentNullException(nameof(innerQueue));
        PartitionStrategy = partitionStrategy ?? throw new ArgumentNullException(nameof(partitionStrategy));
    }

    /// <inheritdoc />
    public IPartitionStrategy PartitionStrategy { get; }

    // ---- IQueue delegation ----

    /// <inheritdoc />
    public IMessageStore Store => _innerQueue.Store;

    /// <inheritdoc />
    public string[] Queues => _innerQueue.Queues;

    /// <inheritdoc />
    public IPEndPoint Endpoint => _innerQueue.Endpoint;

    /// <inheritdoc />
    public void CreateQueue(string queueName) => _innerQueue.CreateQueue(queueName);

    /// <inheritdoc />
    public void Start() => _innerQueue.Start();

    /// <inheritdoc />
    public IAsyncEnumerable<IMessageContext> Receive(string queueName, int pollIntervalInMilliseconds = 200,
        CancellationToken cancellationToken = default)
        => _innerQueue.Receive(queueName, pollIntervalInMilliseconds, cancellationToken);

    /// <inheritdoc />
    public IAsyncEnumerable<IBatchQueueContext> ReceiveBatch(string queueName,
        int maxMessages = 0, int batchTimeoutInMilliseconds = 0, int pollIntervalInMilliseconds = 200,
        CancellationToken cancellationToken = default)
        => _innerQueue.ReceiveBatch(queueName, maxMessages, batchTimeoutInMilliseconds, pollIntervalInMilliseconds, cancellationToken);

    /// <inheritdoc />
    public void ReceiveLater(Message message, TimeSpan timeSpan)
        => _innerQueue.ReceiveLater(message, timeSpan);

    /// <inheritdoc />
    public void ReceiveLater(Message message, DateTimeOffset time)
        => _innerQueue.ReceiveLater(message, time);

    /// <inheritdoc />
    public void MoveToQueue(string queueName, Message message)
        => _innerQueue.MoveToQueue(queueName, message);

    /// <inheritdoc />
    public void Send(params Message[] messages)
        => _innerQueue.Send(messages);

    /// <inheritdoc />
    public void Send(Message message)
        => _innerQueue.Send(message);

    /// <inheritdoc />
    /// <remarks>
    /// If the message's queue has partitions, the message is automatically routed to the
    /// appropriate partition based on the configured <see cref="IPartitionStrategy"/>.
    /// Otherwise, the message is enqueued to the inner queue as-is.
    /// </remarks>
    public void Enqueue(Message message)
    {
        var queueName = message.QueueString;
        if (queueName != null)
        {
            var partitionCount = GetPartitionCount(queueName);
            if (partitionCount > 0)
            {
                var partition = PartitionStrategy.GetPartition(message, partitionCount);
                EnqueueToPartition(message, queueName, partition);
                return;
            }
        }
        _innerQueue.Enqueue(message);
    }

    /// <inheritdoc />
    public void Send<T>(
        T content,
        string? destinationUri = null,
        string? queueName = null,
        Dictionary<string, string>? headers = null,
        DateTime? deliverBy = null,
        int? maxAttempts = null,
        string? partitionKey = null)
        => _innerQueue.Send(content, destinationUri, queueName, headers, deliverBy, maxAttempts, partitionKey);

    /// <inheritdoc />
    /// <remarks>
    /// The content is serialized using the inner queue's content serializer and then
    /// routed through the partition-aware <see cref="Enqueue(Message)"/> overload.
    /// </remarks>
    public void Enqueue<T>(
        T content,
        string? queueName = null,
        Dictionary<string, string>? headers = null,
        string? partitionKey = null)
    {
        var serializer = (_innerQueue as Queue)?._contentSerializer ?? Serialization.JsonContentSerializer.Default;
        var message = Message.Create(
            content,
            contentSerializer: serializer,
            queue: queueName,
            headers: headers,
            partitionKey: partitionKey);
        Enqueue(message);
    }

    /// <inheritdoc />
    public int RequeueDeadLetterMessages()
        => _innerQueue.RequeueDeadLetterMessages();

    /// <inheritdoc />
    public void Dispose()
    {
        foreach (var semaphore in _partitionLocks.Values)
            semaphore.Dispose();
        _partitionLocks.Clear();
        _innerQueue.Dispose();
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        foreach (var semaphore in _partitionLocks.Values)
            semaphore.Dispose();
        _partitionLocks.Clear();
        await _innerQueue.DisposeAsync();
    }

    // ---- IPartitionedQueue operations ----

    /// <inheritdoc />
    public int GetPartitionCount(string queueName)
    {
        if (_partitionCounts.TryGetValue(queueName, out var count))
            return count;

        // Discover partition count from existing queues
        var allQueues = _innerQueue.Queues;
        var prefix = queueName + PartitionConstants.PartitionSeparator;
        var discovered = allQueues.Count(q => q.StartsWith(prefix, StringComparison.Ordinal));

        if (discovered > 0)
        {
            _partitionCounts.TryAdd(queueName, discovered);
            return discovered;
        }

        return 0;
    }

    /// <inheritdoc />
    public void CreatePartitionedQueue(string queueName, int partitionCount)
    {
        if (partitionCount <= 0)
            throw new ArgumentOutOfRangeException(nameof(partitionCount), "Partition count must be greater than zero.");

        for (var i = 0; i < partitionCount; i++)
        {
            var partitionQueueName = PartitionConstants.FormatPartitionQueueName(queueName, i);
            _innerQueue.CreateQueue(partitionQueueName);
        }

        _partitionCounts[queueName] = partitionCount;
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<IMessageContext> ReceiveFromPartition(string queueName, int partition,
        int pollIntervalInMilliseconds = 200,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ValidatePartition(queueName, partition);
        var partitionQueueName = PartitionConstants.FormatPartitionQueueName(queueName, partition);
        var partitionLock = GetPartitionLock(partitionQueueName);

        await partitionLock.WaitAsync(cancellationToken);
        try
        {
            await foreach (var ctx in _innerQueue.Receive(partitionQueueName, pollIntervalInMilliseconds, cancellationToken))
            {
                yield return ctx;
            }
        }
        finally
        {
            partitionLock.Release();
        }
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<IBatchQueueContext> ReceiveBatchFromPartition(string queueName, int partition,
        int maxMessages = 0, int batchTimeoutInMilliseconds = 0, int pollIntervalInMilliseconds = 200,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ValidatePartition(queueName, partition);
        var partitionQueueName = PartitionConstants.FormatPartitionQueueName(queueName, partition);
        var partitionLock = GetPartitionLock(partitionQueueName);

        await partitionLock.WaitAsync(cancellationToken);
        try
        {
            await foreach (var batch in _innerQueue.ReceiveBatch(partitionQueueName, maxMessages, batchTimeoutInMilliseconds, pollIntervalInMilliseconds, cancellationToken))
            {
                yield return batch;
            }
        }
        finally
        {
            partitionLock.Release();
        }
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<IMessageContext> ReceiveFromPartitions(string queueName, int[] partitions,
        int pollIntervalInMilliseconds = 200,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        foreach (var partition in partitions)
        {
            ValidatePartition(queueName, partition);
        }

        // Merge multiple partition streams by polling each partition in sequence per cycle
        var partitionQueueNames = partitions
            .Select(p => PartitionConstants.FormatPartitionQueueName(queueName, p))
            .ToArray();

        while (!cancellationToken.IsCancellationRequested)
        {
            var foundAny = false;

            foreach (var pqn in partitionQueueNames)
            {
                if (cancellationToken.IsCancellationRequested)
                    yield break;

                var partitionLock = GetPartitionLock(pqn);
                await partitionLock.WaitAsync(cancellationToken);
                try
                {
                    var messages = Store.PersistedIncoming(pqn)
                        .Where(m => m.Queue.Span.SequenceEqual(pqn.AsSpan()))
                        .ToList();

                    foreach (var message in messages)
                    {
                        if (cancellationToken.IsCancellationRequested)
                            yield break;

                        foundAny = true;
                        yield return new MessageContext(message, (Queue)_innerQueue);
                    }
                }
                finally
                {
                    partitionLock.Release();
                }
            }

            if (!foundAny)
            {
                try
                {
                    await Task.Delay(pollIntervalInMilliseconds, cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    yield break;
                }
            }
        }
    }
    /// <inheritdoc />
    public void EnqueueToPartition(Message message, string queueName, int partition)
    {
        ValidatePartition(queueName, partition);
        var partitionQueueName = PartitionConstants.FormatPartitionQueueName(queueName, partition);

        // Create a new message with the partition queue name
        var partitionedMessage = new Message(
            message.Id,
            message.Data,
            partitionQueueName.AsMemory(),
            message.SentAt,
            message.SubQueue,
            message.DestinationUri,
            message.DeliverBy,
            message.MaxAttempts,
            message.Headers,
            partitionKey: message.PartitionKey);

        _innerQueue.Enqueue(partitionedMessage);
    }

    /// <inheritdoc />
    public int ResolvePartition(Message message, string queueName)
    {
        var partitionCount = GetPartitionCount(queueName);
        if (partitionCount == 0)
            throw new InvalidOperationException($"No partitions found for queue '{queueName}'. Call CreatePartitionedQueue first.");

        return PartitionStrategy.GetPartition(message, partitionCount);
    }

    private void ValidatePartition(string queueName, int partition)
    {
        if (partition < 0)
            throw new ArgumentOutOfRangeException(nameof(partition), "Partition index must be non-negative.");

        var partitionCount = GetPartitionCount(queueName);
        if (partitionCount > 0 && partition >= partitionCount)
            throw new ArgumentOutOfRangeException(nameof(partition),
                $"Partition index {partition} is out of range. Queue '{queueName}' has {partitionCount} partitions (0-{partitionCount - 1}).");
    }

    /// <summary>
    /// Gets or creates a per-partition lock that ensures only one consumer can receive
    /// from a given partition at any time. This prevents two workers from picking up the
    /// same batch during rescaling or concurrent access.
    /// </summary>
    private SemaphoreSlim GetPartitionLock(string partitionQueueName)
        => _partitionLocks.GetOrAdd(partitionQueueName, _ => new SemaphoreSlim(1, 1));
}
