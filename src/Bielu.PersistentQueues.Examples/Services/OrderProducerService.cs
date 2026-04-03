using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Bielu.PersistentQueues.Partitioning;
using Microsoft.Extensions.Hosting;

namespace Bielu.PersistentQueues.Examples.Services;

/// <summary>
/// Background service that continuously produces order messages (partitioned)
/// and occasional priority alerts (non-partitioned) to the same queue instance.
/// </summary>
internal sealed class OrderProducerService(
    IPartitionedQueue queue,
    string[] customerIds,
    DemoStats stats) : BackgroundService
{
    private const string OrdersQueue = "orders";
    private const string PriorityQueue = "priority-alerts";

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var rng = new Random(42);
        int orderNo = 0;

        while (!stoppingToken.IsCancellationRequested)
        {
            // Send a burst of order messages routed by customer ID → partition.
            for (int i = 0; i < 20 && !stoppingToken.IsCancellationRequested; i++)
            {
                string customerId = customerIds[rng.Next(customerIds.Length)];
                var msg = Message.Create(
                    data: Encoding.UTF8.GetBytes($"{{\"customerId\":\"{customerId}\",\"orderId\":{++orderNo}}}"),
                    queue: OrdersQueue,
                    partitionKey: customerId);
                queue.EnqueueToPartition(msg, OrdersQueue);
                stats.IncrementOrdersSent();
            }

            // ~20 % of the time, inject a priority alert into the non-partitioned queue.
            if (rng.NextDouble() < 0.2)
            {
                var alert = Message.Create(
                    data: Encoding.UTF8.GetBytes($"ALERT: anomaly detected near order #{orderNo}"),
                    queue: PriorityQueue);
                queue.Enqueue(alert);
                stats.IncrementPrioritySent();
            }

            await Task.Delay(10, stoppingToken);
        }
    }
}
