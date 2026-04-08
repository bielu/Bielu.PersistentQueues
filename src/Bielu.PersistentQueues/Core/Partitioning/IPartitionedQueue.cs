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
    /// Enqueues a message to a specific partition, bypassing the partition strategy.
    /// </summary>
    /// <param name="message">The message to enqueue. The queue name is read from <see cref="Message.QueueString"/>.</param>
    /// <param name="partition">The zero-based partition index.</param>
    void EnqueueToPartition(Message message, int partition);

    /// <summary>
    /// Gets the partition index that a message would be routed to without actually enqueuing it.
    /// </summary>
    /// <param name="message">The message to check. The queue name is read from <see cref="Message.QueueString"/>.</param>
    /// <returns>The zero-based partition index.</returns>
    int ResolvePartition(Message message);

    /// <summary>
    /// Gets the number of persisted messages in a specific partition of a queue.
    /// </summary>
    /// <param name="queueName">The base queue name.</param>
    /// <param name="partition">The zero-based partition index.</param>
    /// <returns>The number of messages in the specified partition.</returns>
    /// <remarks>
    /// This method provides a lightweight way to check whether a partition has messages
    /// without starting a full receive operation. It is useful for workers that need to
    /// skip empty partitions to avoid blocking on partitions with no data.
    /// </remarks>
    long GetPartitionMessageCount(string queueName, int partition);

    /// <summary>
    /// Gets the partition indices that have at least one persisted message (non-empty partitions).
    /// </summary>
    /// <param name="queueName">The base queue name.</param>
    /// <returns>An array of zero-based partition indices that contain messages.</returns>
    /// <remarks>
    /// This method checks every partition of the specified queue and returns only those
    /// that currently have messages. It does not consider lock state — a partition may
    /// be active but currently held by another consumer.
    /// </remarks>
    int[] GetActivePartitions(string queueName);

    /// <summary>
    /// Gets the partition indices that have at least one persisted message and are not
    /// currently locked by another consumer.
    /// </summary>
    /// <param name="queueName">The base queue name.</param>
    /// <returns>An array of zero-based partition indices that contain messages and are not locked.</returns>
    /// <remarks>
    /// This is the recommended method for workers that need to pick a partition to consume from.
    /// It combines the message-count check with the internal partition lock state, so callers
    /// do not need to guess which partitions are available. A partition is considered available
    /// when it has messages <b>and</b> no other consumer currently holds its exclusive lock.
    /// </remarks>
    int[] GetAvailablePartitions(string queueName);
}
