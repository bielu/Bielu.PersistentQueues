using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Bielu.PersistentQueues.Partitioning;
using Microsoft.Extensions.Hosting;

namespace Bielu.PersistentQueues.Examples.Services;

/// <summary>
/// Background service that consumes messages exclusively from a contiguous slice
/// of partitions within the orders queue. Each instance is assigned a distinct
/// slice so workers never overlap. After processing one batch the cursor advances
/// to the next partition in the slice — the "switch between batches" pattern.
/// </summary>
internal sealed class PartitionWorkerService(
    IPartitionedQueue queue,
    int[] partitions,
    DemoStats stats) : BackgroundService
{
    private const string OrdersQueue = "orders";

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            // Ask the queue which of our assigned partitions are available
            // (have messages AND are not locked by another consumer).
            var available = queue.GetAvailablePartitions(OrdersQueue)
                .Where(p => partitions.Contains(p))
                .ToArray();

            if (available.Length == 0)
            {
                // No work right now — sleep briefly and re-check.
                try { await Task.Delay(10, stoppingToken); }
                catch (OperationCanceledException) { break; }
                continue;
            }

            foreach (var partition in available)
            {
                if (stoppingToken.IsCancellationRequested) break;

                await foreach (var batch in queue.ReceiveBatchFromPartition(
                    OrdersQueue,
                    partition,
                    maxMessages: 50,
                    batchTimeoutInMilliseconds: 10,
                    pollIntervalInMilliseconds: 5,
                    cancellationToken: stoppingToken))
                {
                    if (batch.Messages.Length > 0)
                    {
                        stats.AddOrdersProcessed(batch.Messages.Length);
                        batch.SuccessfullyReceived();
                    }
                    break; // take one batch then rotate to the next partition
                }
            }
        }
    }
}
