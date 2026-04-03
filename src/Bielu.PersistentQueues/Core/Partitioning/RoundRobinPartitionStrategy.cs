using System;
using System.Threading;

namespace Bielu.PersistentQueues.Partitioning;

/// <summary>
/// Distributes messages evenly across partitions in round-robin order.
/// </summary>
/// <remarks>
/// Each successive message is routed to the next partition, wrapping around
/// when the last partition is reached. This strategy ignores partition keys
/// and provides the most even distribution of messages across partitions.
/// Thread-safe via atomic counter increments.
/// </remarks>
public class RoundRobinPartitionStrategy : IPartitionStrategy
{
    private int _counter;

    /// <inheritdoc />
    public int GetPartition(Message message, int partitionCount)
    {
        if (partitionCount <= 0)
            throw new ArgumentOutOfRangeException(nameof(partitionCount), "Partition count must be greater than zero.");

        if (partitionCount == 1)
            return 0;

        // Use unsigned modulo to handle int overflow correctly
        var current = (uint)Interlocked.Increment(ref _counter);
        return (int)(current % (uint)partitionCount);
    }
}
