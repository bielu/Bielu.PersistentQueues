using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;

namespace Bielu.PersistentQueues.Examples.Services;

/// <summary>
/// Background service that periodically writes a live-progress line to the console.
/// </summary>
internal sealed class ProgressDisplayService(DemoStats stats) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            Console.Write(
                $"\r  Sent   orders: {stats.OrdersSent,7:N0}  alerts: {stats.PrioritySent,5:N0}  │" +
                $"  Processed  orders: {stats.OrdersProcessed,7:N0}  alerts: {stats.PriorityProcessed,5:N0}   ");
            await Task.Delay(250, stoppingToken);
        }
    }
}
