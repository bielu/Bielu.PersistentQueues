using System.Diagnostics;
using System.Runtime.CompilerServices;
using Bielu.PersistentQueues.OpenTelemetry.Instrumentation.Metrics;
using Bielu.PersistentQueues.OpenTelemetry.Instrumentation.Tracing;
using Bielu.PersistentQueues.Partitioning;

namespace Bielu.PersistentQueues.OpenTelemetry.Instrumentation;

/// <summary>
/// OpenTelemetry decorator for <see cref="IPartitionedQueue"/> that adds metrics and tracing
/// for all partition-specific operations.
/// </summary>
public class PartitionedQueueOtelDecorator : PersistentQueueOtelDecorator, IPartitionedQueue
{
    private readonly IPartitionedQueue _partitionedQueue;
    private readonly QueueMetrics _metrics;
    private readonly QueueActivitySource _activitySource;

    public PartitionedQueueOtelDecorator(
        IPartitionedQueue partitionedQueue,
        QueueMetrics queueMetrics,
        QueueActivitySource activitySource)
        : base(partitionedQueue, queueMetrics, activitySource)
    {
        _partitionedQueue = partitionedQueue;
        _metrics = queueMetrics;
        _activitySource = activitySource;
    }

    /// <inheritdoc />
    public int GetPartitionCount(string queueName)
    {
        return _partitionedQueue.GetPartitionCount(queueName);
    }

    /// <inheritdoc />
    public IPartitionStrategy PartitionStrategy => _partitionedQueue.PartitionStrategy;

    /// <inheritdoc />
    public void CreatePartitionedQueue(string queueName, int partitionCount)
    {
        using var activity = _activitySource.StartActivity(ActivityNames.CreatePartitionedQueue, ActivityKind.Internal);
        QueueActivitySource.SetQueueTags(activity, queueName);
        QueueActivitySource.SetPartitionCountTag(activity, partitionCount);

        try
        {
            _partitionedQueue.CreatePartitionedQueue(queueName, partitionCount);
            _metrics.RecordPartitionCreated(queueName, partitionCount);
        }
        catch (Exception ex)
        {
            _metrics.RecordOperationError("CreatePartitionedQueue", queueName);
            _activitySource.RecordException(activity, ex);
            throw;
        }
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<IMessageContext> ReceiveFromPartition(string queueName, int partition,
        int pollIntervalInMilliseconds = 200,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        using var activity = _activitySource.StartActivity(ActivityNames.ReceiveFromPartition, ActivityKind.Consumer);
        QueueActivitySource.SetPartitionTags(activity, queueName, partition);
        QueueActivitySource.SetPollIntervalTag(activity, pollIntervalInMilliseconds);

        await foreach (var messageContext in _partitionedQueue.ReceiveFromPartition(
                           queueName, partition, pollIntervalInMilliseconds, cancellationToken))
        {
            _metrics.RecordPartitionReceived(1, queueName, partition);

            using var messageActivity =
                _activitySource.StartActivity(ActivityNames.ProcessMessage, ActivityKind.Consumer);
            QueueActivitySource.SetMessageTags(messageActivity, messageContext.Message.Id.MessageIdentifier, queueName);
            QueueActivitySource.SetPartitionTags(messageActivity, queueName, partition);

            yield return messageContext;
        }
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<IBatchQueueContext> ReceiveBatchFromPartition(string queueName, int partition,
        int maxMessages = 0, int batchTimeoutInMilliseconds = 0, int pollIntervalInMilliseconds = 200,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        using var activity = _activitySource.StartActivity(ActivityNames.ReceiveBatchFromPartition, ActivityKind.Consumer);
        QueueActivitySource.SetPartitionTags(activity, queueName, partition);
        QueueActivitySource.SetPollIntervalTag(activity, pollIntervalInMilliseconds);
        activity?.SetTag("max.messages", maxMessages);
        activity?.SetTag("batch.timeout", batchTimeoutInMilliseconds);

        await foreach (var batchContext in _partitionedQueue.ReceiveBatchFromPartition(
                           queueName, partition, maxMessages, batchTimeoutInMilliseconds,
                           pollIntervalInMilliseconds, cancellationToken))
        {
            var batchSize = batchContext.Messages.Length;
            _metrics.RecordPartitionReceived(batchSize, queueName, partition);
            _metrics.RecordBatchSize(batchSize, queueName);

            using var batchActivity =
                _activitySource.StartActivity(ActivityNames.ProcessBatch, ActivityKind.Consumer);
            QueueActivitySource.SetBatchTags(batchActivity, batchSize, queueName);
            QueueActivitySource.SetPartitionTags(batchActivity, queueName, partition);

            yield return batchContext;
        }
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<IMessageContext> ReceiveFromPartitions(string queueName, int[] partitions,
        int pollIntervalInMilliseconds = 200,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        using var activity = _activitySource.StartActivity(ActivityNames.ReceiveFromPartitions, ActivityKind.Consumer);
        QueueActivitySource.SetQueueTags(activity, queueName);
        activity?.SetTag("partitions", string.Join(",", partitions));
        QueueActivitySource.SetPollIntervalTag(activity, pollIntervalInMilliseconds);

        await foreach (var messageContext in _partitionedQueue.ReceiveFromPartitions(
                           queueName, partitions, pollIntervalInMilliseconds, cancellationToken))
        {
            _metrics.RecordMessagesReceived(1, queueName);

            yield return messageContext;
        }
    }

    /// <inheritdoc />
    public void EnqueueToPartition(Message message, string queueName)
    {
        using var activity = _activitySource.StartActivity(ActivityNames.EnqueueToPartition, ActivityKind.Producer);
        QueueActivitySource.SetQueueTags(activity, queueName);
        QueueActivitySource.SetMessageTags(activity, message.Id.MessageIdentifier, queueName);

        try
        {
            var startTime = Stopwatch.GetTimestamp();
            var partition = _partitionedQueue.ResolvePartition(message, queueName);

            QueueActivitySource.SetPartitionTags(activity, queueName, partition, message.PartitionKeyString);

            _partitionedQueue.EnqueueToPartition(message, queueName);

            var elapsed = Stopwatch.GetElapsedTime(startTime).TotalMilliseconds;
            _metrics.RecordPartitionEnqueued(queueName, partition, message.PartitionKeyString);
            _metrics.RecordPartitionEnqueueDuration(elapsed, queueName, partition);
        }
        catch (Exception ex)
        {
            _metrics.RecordOperationError("EnqueueToPartition", queueName);
            _activitySource.RecordException(activity, ex);
            throw;
        }
    }

    /// <inheritdoc />
    public void EnqueueToPartition(Message message, string queueName, int partition)
    {
        using var activity = _activitySource.StartActivity(ActivityNames.EnqueueToPartition, ActivityKind.Producer);
        QueueActivitySource.SetPartitionTags(activity, queueName, partition, message.PartitionKeyString);
        QueueActivitySource.SetMessageTags(activity, message.Id.MessageIdentifier, queueName);

        try
        {
            var startTime = Stopwatch.GetTimestamp();

            _partitionedQueue.EnqueueToPartition(message, queueName, partition);

            var elapsed = Stopwatch.GetElapsedTime(startTime).TotalMilliseconds;
            _metrics.RecordPartitionEnqueued(queueName, partition, message.PartitionKeyString);
            _metrics.RecordPartitionEnqueueDuration(elapsed, queueName, partition);
        }
        catch (Exception ex)
        {
            _metrics.RecordOperationError("EnqueueToPartition", queueName);
            _activitySource.RecordException(activity, ex);
            throw;
        }
    }

    /// <inheritdoc />
    public int ResolvePartition(Message message, string queueName)
    {
        using var activity = _activitySource.StartActivity(ActivityNames.ResolvePartition, ActivityKind.Internal);
        QueueActivitySource.SetQueueTags(activity, queueName);

        try
        {
            var partition = _partitionedQueue.ResolvePartition(message, queueName);
            QueueActivitySource.SetPartitionTags(activity, queueName, partition, message.PartitionKeyString);
            return partition;
        }
        catch (Exception ex)
        {
            _metrics.RecordOperationError("ResolvePartition", queueName);
            _activitySource.RecordException(activity, ex);
            throw;
        }
    }
}
