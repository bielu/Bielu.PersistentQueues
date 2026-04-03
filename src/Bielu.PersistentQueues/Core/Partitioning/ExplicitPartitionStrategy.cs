using System;

namespace Bielu.PersistentQueues.Partitioning;

/// <summary>
/// Routes messages to partitions based on a numeric partition key provided by the caller.
/// </summary>
/// <remarks>
/// This strategy expects the message's partition key to be a numeric string representing
/// the desired partition index. If the partition key is empty or not a valid number,
/// the message is routed to partition 0. Values exceeding the partition count are
/// wrapped using modulo arithmetic.
/// </remarks>
public class ExplicitPartitionStrategy : IPartitionStrategy
{
    /// <inheritdoc />
    public int GetPartition(Message message, int partitionCount)
    {
        if (partitionCount <= 0)
            throw new ArgumentOutOfRangeException(nameof(partitionCount), "Partition count must be greater than zero.");

        if (partitionCount == 1)
            return 0;

        if (message.PartitionKey.IsEmpty)
            return 0;

        if (int.TryParse(message.PartitionKey.Span, out var partition))
        {
            // Normalize: handle negative values and values >= partitionCount
            partition = ((partition % partitionCount) + partitionCount) % partitionCount;
            return partition;
        }

        return 0;
    }
}
