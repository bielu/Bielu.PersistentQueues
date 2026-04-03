// =============================================================================
// Bielu.PersistentQueues – Mix & Match: Partitioned + Non-Partitioned Queues
// =============================================================================
//
// This example demonstrates:
//
//  1. CsvListPartitionStrategy – routes messages by looking up the partition
//     key in a pre-loaded list of customer IDs. IDs are mapped to one of 2 000
//     partitions based on their position in the list.
//
//  2. 2 000 partitions / 30 workers – each worker is a BackgroundService
//     assigned a slice of ~67 partitions. It rotates through them, taking one
//     batch per visit before advancing to the next partition.
//
//  3. Mix and match – the SAME IPartitionedQueue instance handles both:
//       • "orders"          – partitioned queue (2 000 sub-queues)
//       • "priority-alerts" – plain FIFO queue with no partition logic
//     Each concern lives in its own BackgroundService; they never share a
//     foreach loop.
//
// =============================================================================

using System;
using System.IO;
using System.Linq;
using Bielu.PersistentQueues;
using Bielu.PersistentQueues.Examples.Services;
using Bielu.PersistentQueues.Examples.Strategies;
using Bielu.PersistentQueues.Partitioning;
using Bielu.PersistentQueues.Storage.LMDB;
using LightningDB;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

// ── Configuration ─────────────────────────────────────────────────────────────
const int PartitionCount = 2000;
const int WorkerCount = 30;
const int TotalCustomers = 10_000;
const string OrdersQueue = "orders";
const string PriorityQueue = "priority-alerts";
const int DemoSeconds = 10;

// ── Build a CSV list of customer IDs ─────────────────────────────────────────
string[] customerIds = Enumerable.Range(1, TotalCustomers)
    .Select(i => $"CUST-{i:D5}")
    .ToArray();

string csvIds = string.Join(',', customerIds);

// ── Compute worker partition slices ───────────────────────────────────────────
// Worker 0 → partitions   0 –  66
// Worker 1 → partitions  67 – 133  …  Worker 29 → partitions 1934 – 1999
int partitionsPerWorker = (int)Math.Ceiling((double)PartitionCount / WorkerCount);

int[][] workerPartitions = Enumerable.Range(0, WorkerCount)
    .Select(w =>
        Enumerable.Range(w * partitionsPerWorker, partitionsPerWorker)
            .Where(p => p < PartitionCount)
            .ToArray())
    .Where(ps => ps.Length > 0)
    .ToArray();

var dbPath = Path.Combine(Path.GetTempPath(), $"pq-example-{Guid.NewGuid():N}");
Directory.CreateDirectory(dbPath);

Console.WriteLine("══════════════════════════════════════════════════════════════════════");
Console.WriteLine(" Bielu.PersistentQueues – Partitioned + Non-Partitioned (Mix & Match)");
Console.WriteLine("══════════════════════════════════════════════════════════════════════");
Console.WriteLine();
Console.WriteLine($"  Customer IDs   : {TotalCustomers:N0}  (routing list loaded from CSV)");
Console.WriteLine($"  Partitions     : {PartitionCount:N0}  (orders queue)");
Console.WriteLine($"  Workers        : {WorkerCount}  (~{partitionsPerWorker} partitions / worker)");
Console.WriteLine($"  Unpartitioned  : {PriorityQueue}  (same queue instance – mix and match!)");
Console.WriteLine($"  Demo duration  : {DemoSeconds} seconds");
Console.WriteLine();
Console.WriteLine($"  Worker partition assignment ({workerPartitions.Length} workers):");
foreach (var (ps, idx) in workerPartitions.Select((ps, i) => (ps, i)))
    Console.WriteLine($"    Worker {idx,2}: partitions {ps[0],4} – {ps[^1],4}  ({ps.Length} partitions)");
Console.WriteLine();
Console.Write("  Building host … ");

// ── Generic host setup ────────────────────────────────────────────────────────
var builder = Host.CreateApplicationBuilder(args);

builder.Services.Configure<HostOptions>(opts =>
    opts.ShutdownTimeout = TimeSpan.FromSeconds(5));

// Shared live-stats counter (thread-safe via Interlocked).
var stats = new DemoStats();
builder.Services.AddSingleton(stats);

// The partitioned queue is registered as a singleton so all services share the
// same instance. The DI container calls DisposeAsync() on it when the host stops.
builder.Services.AddSingleton<IPartitionedQueue>(_ =>
{
    var strategy = new CsvListPartitionStrategy(csvIds);
    var config = new QueueConfiguration()
        .WithDefaults()
        .StoreWithLmdb(dbPath, new EnvironmentConfiguration
        {
            // 2 000 partition sub-queues + 1 non-partitioned queue: LMDB needs one
            // database per logical queue, so MaxDatabases must exceed the total count.
            MaxDatabases = 5000,
            MapSize = StorageSize.MB(512)   // 512 MB virtual address space for the demo
        });

    IPartitionedQueue q = new PartitionedQueue(config.BuildQueue(), strategy);
    q.CreatePartitionedQueue(OrdersQueue, PartitionCount);
    q.CreateQueue(PriorityQueue);
    q.Start();
    return q;
});

// Producer: sends order messages (partitioned) and priority alerts (non-partitioned).
builder.Services.AddSingleton<IHostedService>(sp => new OrderProducerService(
    sp.GetRequiredService<IPartitionedQueue>(), customerIds, sp.GetRequiredService<DemoStats>()));

// Consumer for the non-partitioned priority-alerts queue.
// Kept in its own service so the foreach loop never mixes queue concerns.
builder.Services.AddSingleton<IHostedService>(sp => new PriorityAlertConsumerService(
    sp.GetRequiredService<IPartitionedQueue>(), sp.GetRequiredService<DemoStats>()));

// One BackgroundService per worker partition slice.
// Each service owns exactly one foreach loop over its assigned partitions.
foreach (var slices in workerPartitions)
{
    builder.Services.AddSingleton<IHostedService>(sp => new PartitionWorkerService(
        sp.GetRequiredService<IPartitionedQueue>(), slices, sp.GetRequiredService<DemoStats>()));
}

// Live progress display.
builder.Services.AddSingleton<IHostedService>(sp =>
    new ProgressDisplayService(sp.GetRequiredService<DemoStats>()));

Console.WriteLine("ready.");
Console.WriteLine();

var host = builder.Build();

// Auto-stop the host after the demo window elapses.
var lifetime = host.Services.GetRequiredService<IHostApplicationLifetime>();
_ = Task.Delay(TimeSpan.FromSeconds(DemoSeconds))
    .ContinueWith(_ =>
    {
        try { lifetime.StopApplication(); }
        catch (ObjectDisposedException) { /* host already stopped */ }
    }, TaskScheduler.Default);

await host.RunAsync();

// ── Results ───────────────────────────────────────────────────────────────────
Console.WriteLine();
Console.WriteLine();
Console.WriteLine("══════════════════════════════════════════════════════════════════════");
Console.WriteLine(" Results");
Console.WriteLine("══════════════════════════════════════════════════════════════════════");
Console.WriteLine($"  Orders  – sent {stats.OrdersSent,8:N0}   processed {stats.OrdersProcessed,8:N0}");
Console.WriteLine($"  Alerts  – sent {stats.PrioritySent,8:N0}   processed {stats.PriorityProcessed,8:N0}");
Console.WriteLine();
Console.WriteLine($"  Partition strategy : CsvListPartitionStrategy ({TotalCustomers:N0} customer IDs)");
Console.WriteLine($"  Partitions         : {PartitionCount:N0}  ({PartitionCount} physical sub-queues under \"{OrdersQueue}\")");
Console.WriteLine($"  Workers            : {workerPartitions.Length}  × ~{partitionsPerWorker} partitions each");
Console.WriteLine($"  Non-partitioned    : \"{PriorityQueue}\"  (same IPartitionedQueue instance)");
Console.WriteLine("══════════════════════════════════════════════════════════════════════");

// ── Cleanup ───────────────────────────────────────────────────────────────────
// The IPartitionedQueue singleton is disposed by the DI container on host shutdown.
try { Directory.Delete(dbPath, recursive: true); } catch { /* best effort */ }

Console.WriteLine();
Console.WriteLine("  Done. Press any key to exit.");
try { Console.ReadKey(intercept: true); }
catch (InvalidOperationException) { /* non-interactive environment – just exit */ }

