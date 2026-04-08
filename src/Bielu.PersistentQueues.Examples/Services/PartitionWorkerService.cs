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
        int cursor = 0;

        while (!stoppingToken.IsCancellationRequested)
        {
            int partition = partitions[cursor % partitions.Length];

            // Skip empty partitions to avoid blocking on ReceiveBatchFromPartition,
            // which never yields for partitions with no messages.
            if (queue.GetPartitionMessageCount(OrdersQueue, partition) == 0)
            {
                cursor++;
                continue;
            }

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

            cursor++;
        }
    }
}
