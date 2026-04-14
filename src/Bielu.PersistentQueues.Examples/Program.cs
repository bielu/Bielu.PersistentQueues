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
//  2. Dynamic worker pool – PartitionCoordinatorService manages N partition-
//     consumer tasks where N = WorkerPool:WorkerCount in appsettings.json.
//     Edit that value while the app is running to scale workers up or down
//     without restarting.
//
//  3. Mix and match – the SAME IPartitionedQueue instance handles both:
//       • "orders"          – partitioned queue (2 000 sub-queues)
//       • "priority-alerts" – plain FIFO queue with no partition logic
//     Each concern lives in its own BackgroundService.
//
//  4. BatchProcessingLock – a shared SemaphoreSlim(1,1) that ensures only one
//     consumer (partition worker or priority-alert consumer) commits a batch
//     at any moment, preventing concurrent writes.
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
using Microsoft.Extensions.Options;

// ── Configuration ─────────────────────────────────────────────────────────────
const int PartitionCount = 2000;
const int TotalCustomers = 10_000;
const string OrdersQueue = "orders";
const string PriorityQueue = "priority-alerts";
const int DemoSeconds = 10;

// ── Build a CSV list of customer IDs ─────────────────────────────────────────
string[] customerIds = Enumerable.Range(1, TotalCustomers)
    .Select(i => $"CUST-{i:D5}")
    .ToArray();

string csvIds = string.Join(',', customerIds);

var dbPath = Path.Combine(Path.GetTempPath(), $"pq-example-{Guid.NewGuid():N}");
Directory.CreateDirectory(dbPath);

Console.WriteLine("══════════════════════════════════════════════════════════════════════");
Console.WriteLine(" Bielu.PersistentQueues – Partitioned + Non-Partitioned (Mix & Match)");
Console.WriteLine("══════════════════════════════════════════════════════════════════════");
Console.WriteLine();
Console.WriteLine($"  Customer IDs  : {TotalCustomers:N0}  (routing list loaded from CSV)");
Console.WriteLine($"  Partitions    : {PartitionCount:N0}  (orders queue)");
Console.WriteLine($"  Unpartitioned : {PriorityQueue}  (mix and match on same queue instance)");
Console.WriteLine($"  Demo duration : {DemoSeconds} seconds");
Console.WriteLine($"  Workers       : controlled via WorkerPool:WorkerCount in appsettings.json");
Console.WriteLine($"                  (edit live to rescale without restart)");
Console.WriteLine();
Console.Write("  Building host … ");

// ── Generic host setup ────────────────────────────────────────────────────────
var builder = Host.CreateApplicationBuilder(args);

builder.Services.Configure<HostOptions>(opts =>
    opts.ShutdownTimeout = TimeSpan.FromSeconds(5));

// Bind WorkerPoolOptions to appsettings.json "WorkerPool" section.
// IOptionsMonitor<WorkerPoolOptions> reacts to file changes automatically.
builder.Services.Configure<WorkerPoolOptions>(
    builder.Configuration.GetSection(WorkerPoolOptions.Section));

// Shared live-stats counter (thread-safe via Interlocked).
var stats = new DemoStats();
builder.Services.AddSingleton(stats);

// Shared exclusive lock — only one consumer processes a batch at a time.
builder.Services.AddSingleton<BatchProcessingLock>();

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
// Uses BatchProcessingLock so it never runs concurrently with a partition worker.
builder.Services.AddSingleton<IHostedService>(sp => new PriorityAlertConsumerService(
    sp.GetRequiredService<IPartitionedQueue>(),
    sp.GetRequiredService<DemoStats>(),
    sp.GetRequiredService<BatchProcessingLock>()));

// Coordinator: manages a dynamic pool of partition-consumer tasks.
// Worker count is read from WorkerPool:WorkerCount (appsettings.json) and
// can be changed at runtime — the coordinator rescales without a restart.
builder.Services.AddSingleton<IHostedService>(sp => new PartitionCoordinatorService(
    sp.GetRequiredService<IPartitionedQueue>(),
    PartitionCount,
    sp.GetRequiredService<DemoStats>(),
    sp.GetRequiredService<BatchProcessingLock>(),
    sp.GetRequiredService<IOptionsMonitor<WorkerPoolOptions>>()));

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

await host.RunAsync().ConfigureAwait(false);

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
Console.WriteLine($"  Non-partitioned    : \"{PriorityQueue}\"  (same IPartitionedQueue instance)");
Console.WriteLine("══════════════════════════════════════════════════════════════════════");

// ── Cleanup ───────────────────────────────────────────────────────────────────
// The IPartitionedQueue singleton is disposed by the DI container on host shutdown.
try { Directory.Delete(dbPath, recursive: true); } catch { /* best effort */ }

Console.WriteLine();
Console.WriteLine("  Done. Press any key to exit.");
try { Console.ReadKey(intercept: true); }
catch (InvalidOperationException) { /* non-interactive environment – just exit */ }

