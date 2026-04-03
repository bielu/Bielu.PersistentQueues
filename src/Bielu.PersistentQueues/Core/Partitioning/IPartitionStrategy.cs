namespace Bielu.PersistentQueues.Partitioning;

/// <summary>
/// Defines a strategy for assigning messages to partitions within a partitioned queue.
/// </summary>
/// <remarks>
/// Partition strategies determine how messages are distributed across partitions.
/// Built-in implementations include hash-based (consistent key routing),
/// round-robin (even distribution), and explicit (caller-specified) strategies.
/// </remarks>
public interface IPartitionStrategy
{
    /// <summary>
    /// Determines the partition index for a given message.
    /// </summary>
    /// <param name="message">The message to route to a partition.</param>
    /// <param name="partitionCount">The total number of partitions available.</param>
    /// <returns>The zero-based partition index the message should be routed to.</returns>
    int GetPartition(Message message, int partitionCount);
}
