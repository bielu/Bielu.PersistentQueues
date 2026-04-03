using System.Threading;
using System.Threading.Tasks;
using Bielu.PersistentQueues.Partitioning;
using Microsoft.Extensions.Hosting;

namespace Bielu.PersistentQueues.Examples.Services;

/// <summary>
/// Background service that consumes messages exclusively from the non-partitioned
/// priority-alerts queue. Kept separate from partition workers so each concern
/// has its own service and its own async foreach loop.
/// </summary>
internal sealed class PriorityAlertConsumerService(
    IPartitionedQueue queue,
    DemoStats stats) : BackgroundService
{
    private const string PriorityQueue = "priority-alerts";

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            await foreach (var batch in queue.ReceiveBatch(
                PriorityQueue,
                maxMessages: 10,
                batchTimeoutInMilliseconds: 100,
                pollIntervalInMilliseconds: 10,
                cancellationToken: stoppingToken))
            {
                if (batch.Messages.Length > 0)
                {
                    stats.AddPriorityProcessed(batch.Messages.Length);
                    batch.SuccessfullyReceived();
                }
                break; // one poll per loop iteration
            }
        }
    }
}
