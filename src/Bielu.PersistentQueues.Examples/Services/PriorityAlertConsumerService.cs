using System.Threading;
using System.Threading.Tasks;
using Bielu.PersistentQueues.Partitioning;
using Microsoft.Extensions.Hosting;

namespace Bielu.PersistentQueues.Examples.Services;

/// <summary>
/// Background service that consumes messages exclusively from the non-partitioned
/// priority-alerts queue. Acquires the shared <see cref="BatchProcessingLock"/>
/// before processing each batch so that no partition worker runs concurrently.
/// </summary>
internal sealed class PriorityAlertConsumerService(
    IPartitionedQueue queue,
    DemoStats stats,
    BatchProcessingLock batchLock) : BackgroundService
{
    private const string PriorityQueue = "priority-alerts";

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            bool lockAcquired = false;
            try
            {
                await batchLock.WaitAsync(stoppingToken).ConfigureAwait(false);
                lockAcquired = true;

                await foreach (var batch in queue.ReceiveBatch(
                    PriorityQueue,
                    maxMessages: 10,
                    batchTimeoutInMilliseconds: 100,
                    pollIntervalInMilliseconds: 10,
                    cancellationToken: stoppingToken).ConfigureAwait(false))
                {
                    if (batch.Messages.Length > 0)
                    {
                        stats.AddPriorityProcessed(batch.Messages.Length);
                        batch.SuccessfullyReceived();
                    }
                    break; // one poll per loop iteration
                }
            }
            catch (OperationCanceledException) { break; }
            finally
            {
                if (lockAcquired) batchLock.Release();
            }
        }
    }
}
