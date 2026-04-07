using System;
using System.Collections.Generic;
using System.Linq;
using Bielu.PersistentQueues.Partitioning;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Bielu.PersistentQueues;

/// <summary>
/// Holds partitioned queue definitions that are collected at configuration time
/// and applied when the <see cref="IPartitionedQueue"/> service is resolved.
/// </summary>
internal class PartitionedQueueDefinitions
{
    public List<(string QueueName, int PartitionCount)> Definitions { get; } = new();
}

/// <summary>
/// Extension methods on <see cref="PersistentQueuesBuilder"/> for configuring partitioned queues via DI.
/// </summary>
public static class PartitionedQueueBuilderExtensions
{
    /// <summary>
    /// Gets or creates the shared <see cref="PartitionedQueueDefinitions"/> singleton
    /// in the service collection so that multiple calls can accumulate definitions.
    /// </summary>
    private static PartitionedQueueDefinitions GetOrAddDefinitions(IServiceCollection services)
    {
        var descriptor = services.FirstOrDefault(d => d.ServiceType == typeof(PartitionedQueueDefinitions));
        if (descriptor?.ImplementationInstance is PartitionedQueueDefinitions existing)
            return existing;

        var definitions = new PartitionedQueueDefinitions();
        services.AddSingleton(definitions);
        return definitions;
    }

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

        // Ensure the definitions holder is registered
        GetOrAddDefinitions(builder.Services);

        builder.Services.TryAddSingleton<IPartitionedQueue>(sp =>
        {
            var innerQueue = sp.GetRequiredService<IQueue>();
            var definitions = sp.GetRequiredService<PartitionedQueueDefinitions>();
            var pq = new PartitionedQueue(innerQueue, strategy);
            foreach (var (queueName, partitionCount) in definitions.Definitions)
            {
                pq.CreatePartitionedQueue(queueName, partitionCount);
            }
            return pq;
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
        builder.CreatePartitionedQueues(partitionedQueues);
        return builder;
    }

    /// <summary>
    /// Specifies partitioned queues to create when the <see cref="IPartitionedQueue"/> service is resolved.
    /// This is the partitioned-queue equivalent of <see cref="PersistentQueuesBuilder.CreateQueues"/>.
    /// Requires <see cref="UsePartitioning"/> to be called (either before or after this method).
    /// </summary>
    /// <param name="builder">The queue builder.</param>
    /// <param name="partitionedQueues">
    /// Array of tuples specifying queues and their partition counts.
    /// For example: <c>("orders", 4)</c> creates 4 partitions for the "orders" queue.
    /// </param>
    /// <returns>The builder for chaining.</returns>
    public static PersistentQueuesBuilder CreatePartitionedQueues(this PersistentQueuesBuilder builder,
        params (string queueName, int partitionCount)[] partitionedQueues)
    {
        if (partitionedQueues.Length == 0)
            return builder;

        var definitions = GetOrAddDefinitions(builder.Services);
        definitions.Definitions.AddRange(partitionedQueues);

        // Also register the underlying partition queues so they are created with CreateQueues
        var partitionQueueNames = partitionedQueues
            .SelectMany(pq => Enumerable.Range(0, pq.partitionCount)
                .Select(i => PartitionConstants.FormatPartitionQueueName(pq.queueName, i)))
            .ToArray();

        builder.CreateQueues(partitionQueueNames);

        return builder;
    }
}
