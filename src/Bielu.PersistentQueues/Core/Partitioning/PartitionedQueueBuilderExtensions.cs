using System;
using System.Linq;
using Bielu.PersistentQueues.Partitioning;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Bielu.PersistentQueues;

/// <summary>
/// Extension methods on <see cref="PersistentQueuesBuilder"/> for configuring partitioned queues via DI.
/// </summary>
public static class PartitionedQueueBuilderExtensions
{
    /// <summary>
    /// Registers partitioning support with the specified strategy.
    /// Registers both <see cref="IPartitionStrategy"/> and <see cref="IPartitionedQueue"/>
    /// in the service collection.
    /// </summary>
    /// <param name="builder">The queue builder.</param>
    /// <param name="partitionStrategy">The partition strategy to use. Defaults to <see cref="HashPartitionStrategy"/>.</param>
    /// <returns>The builder for chaining.</returns>
    public static PersistentQueuesBuilder UsePartitioning(this PersistentQueuesBuilder builder,
        IPartitionStrategy? partitionStrategy = null)
    {
        var strategy = partitionStrategy ?? new HashPartitionStrategy();

        builder.Services.TryAddSingleton(strategy);
        builder.Services.TryAddSingleton<IPartitionedQueue>(sp =>
        {
            var innerQueue = sp.GetRequiredService<IQueue>();
            return new PartitionedQueue(innerQueue, strategy);
        });

        return builder;
    }

    /// <summary>
    /// Registers partitioning support with the specified strategy and creates partitioned queues on startup.
    /// </summary>
    /// <param name="builder">The queue builder.</param>
    /// <param name="partitionStrategy">The partition strategy to use.</param>
    /// <param name="partitionedQueues">
    /// Array of tuples specifying queues and their partition counts.
    /// For example: <c>("orders", 4)</c> creates 4 partitions for the "orders" queue.
    /// </param>
    /// <returns>The builder for chaining.</returns>
    public static PersistentQueuesBuilder UsePartitioning(this PersistentQueuesBuilder builder,
        IPartitionStrategy partitionStrategy, params (string queueName, int partitionCount)[] partitionedQueues)
    {
        builder.UsePartitioning(partitionStrategy);

        if (partitionedQueues.Length > 0)
        {
            // Also register the underlying partition queues so they are created with CreateQueues
            var partitionQueueNames = partitionedQueues
                .SelectMany(pq => Enumerable.Range(0, pq.partitionCount)
                    .Select(i => PartitionConstants.FormatPartitionQueueName(pq.queueName, i)))
                .ToArray();

            builder.CreateQueues(partitionQueueNames);
        }

        return builder;
    }
}
