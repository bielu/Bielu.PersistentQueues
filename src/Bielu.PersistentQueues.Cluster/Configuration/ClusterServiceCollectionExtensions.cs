using System;
using System.Net;
using Bielu.PersistentQueues.Partitioning;
using Bielu.PersistentQueues.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;

namespace Bielu.PersistentQueues.Cluster;

/// <summary>
/// Extension methods for registering cluster support with Microsoft DI.
/// </summary>
public static class ClusterServiceCollectionExtensions
{
    /// <summary>
    /// Adds distributed clustering with replication support to the persistent queues builder.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This extension wraps the registered <see cref="IPartitionedQueue"/> with a
    /// <see cref="ClusteredQueue"/> decorator that adds:
    /// </para>
    /// <list type="bullet">
    ///   <item>Gossip-based cluster membership tracking</item>
    ///   <item>Partition-level primary/replica assignment</item>
    ///   <item>TCP-based message replication (RF=2 by default)</item>
    ///   <item>Automatic failover when a node dies</item>
    /// </list>
    /// <para>
    /// Requires <see cref="PartitionedQueueBuilderExtensions.UsePartitioning"/> to be called first.
    /// </para>
    /// </remarks>
    /// <param name="builder">The queue builder.</param>
    /// <param name="configure">A delegate to configure cluster options.</param>
    /// <returns>The builder for chaining.</returns>
    public static PersistentQueuesBuilder AddClusterSupport(
        this PersistentQueuesBuilder builder,
        Action<ClusterConfiguration> configure)
    {
        var config = new ClusterConfiguration();
        configure(config);

        builder.Services.TryAddSingleton(config);

        builder.Services.TryAddSingleton<IClusterCoordinator>(sp =>
        {
            var queue = sp.GetRequiredService<IQueue>();
            var logger = sp.GetRequiredService<ILogger<GossipClusterCoordinator>>();
            return new GossipClusterCoordinator(config, queue.Endpoint, logger);
        });

        builder.Services.TryAddSingleton<IPartitionAssignment>(sp =>
        {
            var logger = sp.GetRequiredService<ILogger<PartitionAssignmentTable>>();
            return new PartitionAssignmentTable(logger);
        });

        builder.Services.TryAddSingleton<IReplicationProtocol>(sp =>
        {
            var store = sp.GetRequiredService<IMessageStore>();
            var queue = sp.GetRequiredService<IQueue>();
            var logger = sp.GetRequiredService<ILogger<TcpReplicationProtocol>>();
            // Replication endpoint is queue port + gossip offset + 1
            var replicationEndpoint = new IPEndPoint(
                queue.Endpoint.Address,
                queue.Endpoint.Port + config.GossipPortOffset + 1);
            return new TcpReplicationProtocol(store, replicationEndpoint, logger);
        });

        // Decorate IPartitionedQueue with ClusteredQueue
        builder.Services.AddSingleton<IPartitionedQueue>(sp =>
        {
            // Resolve the inner partitioned queue
            // Note: The inner IPartitionedQueue was already registered by UsePartitioning().
            // We need to build it manually to avoid circular resolution.
            var innerQueue = sp.GetRequiredService<IQueue>();
            var strategy = sp.GetRequiredService<IPartitionStrategy>();
            var innerPartitioned = new PartitionedQueue(innerQueue, strategy);

            var coordinator = sp.GetRequiredService<IClusterCoordinator>();
            var assignment = sp.GetRequiredService<IPartitionAssignment>();
            var replication = sp.GetRequiredService<IReplicationProtocol>();
            var logger = sp.GetRequiredService<ILogger<ClusteredQueue>>();

            return new ClusteredQueue(innerPartitioned, coordinator, assignment, replication, config, logger);
        });

        return builder;
    }
}
