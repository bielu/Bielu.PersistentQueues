using System;
using Bielu.PersistentQueues.Partitioning;

namespace Bielu.PersistentQueues;

/// <summary>
/// Extension methods on <see cref="QueueConfiguration"/> for creating partitioned queues.
/// </summary>
public static class PartitionedQueueConfigurationExtensions
{
    /// <summary>
    /// Builds a partitioned queue with the specified partition strategy.
    /// </summary>
    /// <param name="configuration">The queue configuration.</param>
    /// <param name="partitionStrategy">The strategy to use for routing messages to partitions.</param>
    /// <returns>A configured <see cref="IPartitionedQueue"/>.</returns>
    public static IPartitionedQueue BuildPartitionedQueue(this QueueConfiguration configuration,
        IPartitionStrategy? partitionStrategy = null)
    {
        var innerQueue = configuration.BuildQueue();
        return new PartitionedQueue(innerQueue, partitionStrategy ?? new HashPartitionStrategy());
    }

    /// <summary>
    /// Builds, creates partitioned queues, and starts the queue.
    /// </summary>
    /// <param name="configuration">The queue configuration.</param>
    /// <param name="queueName">The base queue name.</param>
    /// <param name="partitionCount">The number of partitions to create.</param>
    /// <param name="partitionStrategy">The strategy to use for routing messages. Defaults to <see cref="HashPartitionStrategy"/>.</param>
    /// <returns>A configured and started <see cref="IPartitionedQueue"/>.</returns>
    public static IPartitionedQueue BuildAndStartPartitioned(this QueueConfiguration configuration,
        string queueName, int partitionCount, IPartitionStrategy? partitionStrategy = null)
    {
        var partitionedQueue = configuration.BuildPartitionedQueue(partitionStrategy);
        partitionedQueue.CreatePartitionedQueue(queueName, partitionCount);
        partitionedQueue.Start();
        return partitionedQueue;
    }
}
