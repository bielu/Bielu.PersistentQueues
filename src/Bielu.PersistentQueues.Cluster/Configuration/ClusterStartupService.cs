using System.Threading;
using System.Threading.Tasks;
using Bielu.PersistentQueues.Partitioning;
using Microsoft.Extensions.Hosting;

namespace Bielu.PersistentQueues.Cluster;

/// <summary>
/// Starts the cluster components of the registered <see cref="ClusteredPartitionedQueueDecorator"/>
/// when the host starts.
/// </summary>
internal sealed class ClusterStartupService(IPartitionedQueue partitionedQueue) : IHostedService
{
    public Task StartAsync(CancellationToken cancellationToken)
    {
        if (partitionedQueue is ClusteredPartitionedQueueDecorator clustered)
            clustered.StartCluster();
        return Task.CompletedTask;
    }

    // Leaving the cluster happens when the container disposes the decorator.
    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
