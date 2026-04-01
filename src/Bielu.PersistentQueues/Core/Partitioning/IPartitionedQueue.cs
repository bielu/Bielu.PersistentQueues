using System.Collections.Generic;
using System.Threading;

namespace Bielu.PersistentQueues.Partitioning;

/// <summary>
/// Defines the interface for a partitioned message queue, inspired by Kafka's partitioning model.
/// </summary>
/// <remarks>
/// A partitioned queue divides a logical queue into multiple partitions. Each partition
/// is an ordered sequence of messages. Messages are routed to partitions via a configurable
/// <see cref="IPartitionStrategy"/> (e.g., hash-based, round-robin, or explicit).
/// Consumers can receive from all partitions, a specific partition, or a subset of partitions.
/// </remarks>
public interface IPartitionedQueue : IQueue
{
    /// <summary>
    /// Gets the number of partitions for the specified queue.
    /// </summary>
    /// <param name="queueName">The base queue name.</param>
    /// <returns>The number of partitions.</returns>
    int GetPartitionCount(string queueName);

    /// <summary>
    /// Gets the partition strategy used for routing messages.
    /// </summary>
    IPartitionStrategy PartitionStrategy { get; }

    /// <summary>
    /// Creates a new partitioned queue with the specified number of partitions.
    /// </summary>
    /// <param name="queueName">The base queue name.</param>
    /// <param name="partitionCount">The number of partitions to create.</param>
    void CreatePartitionedQueue(string queueName, int partitionCount);

    /// <summary>
    /// Receives messages from a specific partition of a queue as an asynchronous stream.
    /// </summary>
    /// <param name="queueName">The base queue name.</param>
    /// <param name="partition">The zero-based partition index to receive from.</param>
    /// <param name="pollIntervalInMilliseconds">The period to rest before checking for new messages.</param>
    /// <param name="cancellationToken">A token to cancel the receive operation.</param>
    /// <returns>An asynchronous stream of <see cref="IMessageContext"/> objects from the specified partition.</returns>
    IAsyncEnumerable<IMessageContext> ReceiveFromPartition(string queueName, int partition,
        int pollIntervalInMilliseconds = 200,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Receives batches of messages from a specific partition as an asynchronous stream.
    /// </summary>
    /// <param name="queueName">The base queue name.</param>
    /// <param name="partition">The zero-based partition index to receive from.</param>
    /// <param name="maxMessages">The maximum number of messages per batch.</param>
    /// <param name="batchTimeoutInMilliseconds">Time in milliseconds to keep collecting messages before yielding a batch.</param>
    /// <param name="pollIntervalInMilliseconds">The period to rest before checking for new messages.</param>
    /// <param name="cancellationToken">A token to cancel the receive operation.</param>
    /// <returns>An asynchronous stream of <see cref="IBatchQueueContext"/> objects from the specified partition.</returns>
    IAsyncEnumerable<IBatchQueueContext> ReceiveBatchFromPartition(string queueName, int partition,
        int maxMessages = 0, int batchTimeoutInMilliseconds = 0, int pollIntervalInMilliseconds = 200,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Receives messages from a subset of partitions as an asynchronous stream.
    /// </summary>
    /// <param name="queueName">The base queue name.</param>
    /// <param name="partitions">The zero-based partition indices to receive from.</param>
    /// <param name="pollIntervalInMilliseconds">The period to rest before checking for new messages.</param>
    /// <param name="cancellationToken">A token to cancel the receive operation.</param>
    /// <returns>An asynchronous stream of <see cref="IMessageContext"/> objects from the specified partitions.</returns>
    IAsyncEnumerable<IMessageContext> ReceiveFromPartitions(string queueName, int[] partitions,
        int pollIntervalInMilliseconds = 200,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Enqueues a message to the appropriate partition based on the configured partition strategy.
    /// </summary>
    /// <param name="message">The message to enqueue.</param>
    /// <param name="queueName">The base queue name.</param>
    /// <remarks>
    /// The partition is determined by the <see cref="IPartitionStrategy"/> based on the message's
    /// <see cref="Message.PartitionKey"/>. If no partition key is set, the strategy determines
    /// the default behavior (e.g., round-robin or message-ID-based hashing).
    /// </remarks>
    void EnqueueToPartition(Message message, string queueName);

    /// <summary>
    /// Enqueues a message to a specific partition, bypassing the partition strategy.
    /// </summary>
    /// <param name="message">The message to enqueue.</param>
    /// <param name="queueName">The base queue name.</param>
    /// <param name="partition">The zero-based partition index.</param>
    void EnqueueToPartition(Message message, string queueName, int partition);

    /// <summary>
    /// Gets the partition index that a message would be routed to without actually enqueuing it.
    /// </summary>
    /// <param name="message">The message to check.</param>
    /// <param name="queueName">The base queue name.</param>
    /// <returns>The zero-based partition index.</returns>
    int ResolvePartition(Message message, string queueName);
}
