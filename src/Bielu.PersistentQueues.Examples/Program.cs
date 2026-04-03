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
//  2. 2 000 partitions / 30 workers – each worker is assigned a slice of ~67
//     partitions and rotates through them, taking one batch per visit before
//     advancing to the next partition. This is the "switch between batches"
//     pattern: no worker stays pinned to a single partition.
//
//  3. Mix and match – the SAME IPartitionedQueue instance handles both:
//       • "orders"          – partitioned queue (2 000 sub-queues)
//       • "priority-alerts" – plain FIFO queue with no partition logic
//     Workers check the non-partitioned queue on every rotation via the
//     standard IQueue.ReceiveBatch path, demonstrating that both styles
//     coexist on one queue object.
//
// =============================================================================

using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Bielu.PersistentQueues;
using Bielu.PersistentQueues.Examples.Strategies;
using Bielu.PersistentQueues.Partitioning;
using Bielu.PersistentQueues.Storage.LMDB;
using LightningDB;

// ── Configuration ─────────────────────────────────────────────────────────────
const int PartitionCount = 2000;
const int WorkerCount = 30;
const int TotalCustomers = 10_000;
const string OrdersQueue = "orders";
const string PriorityQueue = "priority-alerts";
const int DemoSeconds = 10;

// ── Build a CSV list of customer IDs ─────────────────────────────────────────
// In a real application you would load this from a database, a config file, or
// a service discovery endpoint.  The strategy uses each ID's position in this
// list to derive a deterministic partition number.
string[] customerIds = Enumerable.Range(1, TotalCustomers)
    .Select(i => $"CUST-{i:D5}")
    .ToArray();

string csvIds = string.Join(',', customerIds);

Console.WriteLine("══════════════════════════════════════════════════════════════════════");
Console.WriteLine(" Bielu.PersistentQueues – Partitioned + Non-Partitioned (Mix & Match)");
Console.WriteLine("══════════════════════════════════════════════════════════════════════");
Console.WriteLine();
Console.WriteLine($"  Customer IDs   : {TotalCustomers:N0}  (routing list loaded from CSV)");
Console.WriteLine($"  Partitions     : {PartitionCount:N0}  (orders queue)");
Console.WriteLine($"  Workers        : {WorkerCount}  (~{(int)Math.Ceiling((double)PartitionCount / WorkerCount)} partitions / worker)");
Console.WriteLine($"  Unpartitioned  : {PriorityQueue}  (same queue instance – mix and match!)");
Console.WriteLine($"  Demo duration  : {DemoSeconds} seconds");
Console.WriteLine();
Console.Write("  Setting up queues … ");

// ── Queue setup ───────────────────────────────────────────────────────────────
var dbPath = Path.Combine(Path.GetTempPath(), $"pq-example-{Guid.NewGuid():N}");
Directory.CreateDirectory(dbPath);

// CsvListPartitionStrategy maps each customer ID to a partition bucket.
// The 2 000 customer IDs at positions 0–4 all land in partition 0, positions
// 5–9 in partition 1, and so on (10 000 IDs ÷ 2 000 partitions = 5 IDs each).
var strategy = new CsvListPartitionStrategy(csvIds);

var config = new QueueConfiguration()
    .WithDefaults()
    .StoreWithLmdb(dbPath, new EnvironmentConfiguration
    {
        // 2 000 partition sub-queues + 1 non-partitioned queue: LMDB needs one
        // database per logical queue, so MaxDatabases must exceed the total count.
        MaxDatabases = 5000,
        MapSize = 512L * 1024 * 1024   // 512 MB virtual address space for the demo
    });

var innerQueue = config.BuildQueue();

// IPartitionedQueue wraps IQueue with partition-aware methods.
// The underlying IQueue is still reachable via the same reference, which means
// the exact same object handles both partitioned and non-partitioned queues.
IPartitionedQueue queue = new PartitionedQueue(innerQueue, strategy);

// Partitioned queue: creates 2 000 physical sub-queues
//   "orders:partition-0", "orders:partition-1", … "orders:partition-1999"
queue.CreatePartitionedQueue(OrdersQueue, PartitionCount);

// Non-partitioned queue: a plain FIFO queue on the same queue instance.
// Access it through standard IQueue methods (Enqueue / ReceiveBatch) via the
// IPartitionedQueue reference – no partition routing involved.
queue.CreateQueue(PriorityQueue);

queue.Start();
Console.WriteLine("ready.");
Console.WriteLine();

// ── Worker partition assignment ───────────────────────────────────────────────
// Divide the 2 000 partitions evenly across 30 workers.
// Worker 0 → partitions   0 –  66
// Worker 1 → partitions  67 – 133
// …
// Worker 29 → partitions 1934 – 1999
int partitionsPerWorker = (int)Math.Ceiling((double)PartitionCount / WorkerCount);

int[][] workerPartitions = Enumerable.Range(0, WorkerCount)
    .Select(w =>
        Enumerable.Range(w * partitionsPerWorker, partitionsPerWorker)
            .Where(p => p < PartitionCount)
            .ToArray())
    .Where(ps => ps.Length > 0)
    .ToArray();

Console.WriteLine($"  Worker partition assignment ({workerPartitions.Length} workers):");
foreach (var (ps, idx) in workerPartitions.Select((ps, i) => (ps, i)))
    Console.WriteLine($"    Worker {idx,2}: partitions {ps[0],4} – {ps[^1],4}  ({ps.Length} partitions)");
Console.WriteLine();

// ── Statistics ────────────────────────────────────────────────────────────────
int ordersSent = 0;
int prioritySent = 0;
int ordersProcessed = 0;
int priorityProcessed = 0;

using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(DemoSeconds));
var ct = cts.Token;

// ── Producer ──────────────────────────────────────────────────────────────────
// Sends order messages keyed by customer ID (partitioned) and occasional
// priority alerts (non-partitioned) through the same queue instance.
var producerTask = Task.Run(async () =>
{
    var rng = new Random(42);
    int orderNo = 0;
    try
    {
        while (!ct.IsCancellationRequested)
        {
            // Send a burst of order messages.
            // Each message carries a customer ID as its partition key.
            // The CsvListPartitionStrategy translates it to one of 2 000 partitions.
            for (int i = 0; i < 20 && !ct.IsCancellationRequested; i++)
            {
                string customerId = customerIds[rng.Next(customerIds.Length)];
                var msg = Message.Create(
                    data: Encoding.UTF8.GetBytes($"{{\"customerId\":\"{customerId}\",\"orderId\":{++orderNo}}}"),
                    queue: OrdersQueue,
                    partitionKey: customerId   // ← drives CsvListPartitionStrategy
                );
                queue.EnqueueToPartition(msg, OrdersQueue);  // ← partitioned path
                Interlocked.Increment(ref ordersSent);
            }

            // ~20 % of the time, inject a priority alert into the non-partitioned queue.
            // This uses IQueue.Enqueue (no partition key, no routing) via the same
            // IPartitionedQueue reference – the "mix and match" in action.
            if (rng.NextDouble() < 0.2)
            {
                var alert = Message.Create(
                    data: Encoding.UTF8.GetBytes($"ALERT: anomaly detected near order #{orderNo}"),
                    queue: PriorityQueue
                );
                queue.Enqueue(alert);   // ← non-partitioned path
                Interlocked.Increment(ref prioritySent);
            }

            await Task.Delay(10, ct);
        }
    }
    catch (OperationCanceledException) { /* normal shutdown */ }
});

// ── Workers ───────────────────────────────────────────────────────────────────
// Each worker:
//   1. Checks the non-partitioned priority-alerts queue first (mix and match).
//   2. Drains one batch from the current assigned partition (partitioned path).
//   3. Advances its cursor to the next partition and repeats.
//
// The cursor advance after every batch is the "switch between batches" behaviour:
// no worker stays on one partition indefinitely – it visits each partition in turn.
Task StartWorker(int[] partitions) => Task.Run(async () =>
{
    int cursor = 0;
    try
    {
        while (!ct.IsCancellationRequested)
        {
            // ── Step 1: non-partitioned priority queue ─────────────────────
            // We call IQueue.ReceiveBatch through the IPartitionedQueue reference.
            // This is the "mix and match": the exact same queue object services
            // both the partitioned orders path and this plain FIFO path.
            // A very short batch window (5 ms) means the worker is never blocked
            // here for long; it picks up any waiting alerts and moves on.
            await foreach (var batch in queue.ReceiveBatch(
                PriorityQueue,
                maxMessages: 10,
                batchTimeoutInMilliseconds: 5,
                pollIntervalInMilliseconds: 5,
                cancellationToken: ct))
            {
                if (batch.Messages.Length > 0)
                {
                    Interlocked.Add(ref priorityProcessed, batch.Messages.Length);
                    batch.SuccessfullyReceived();
                }
                break; // one quick poll per rotation – do not block waiting for alerts
            }

            // ── Step 2: drain one batch from the current assigned partition ──
            // ReceiveBatchFromPartition targets a single physical sub-queue
            // ("orders:partition-N") and returns up to maxMessages in one shot.
            int partition = partitions[cursor % partitions.Length];
            await foreach (var batch in queue.ReceiveBatchFromPartition(
                OrdersQueue,
                partition,
                maxMessages: 50,
                batchTimeoutInMilliseconds: 10,
                pollIntervalInMilliseconds: 5,
                cancellationToken: ct))
            {
                if (batch.Messages.Length > 0)
                {
                    Interlocked.Add(ref ordersProcessed, batch.Messages.Length);
                    batch.SuccessfullyReceived();
                }
                break; // take one batch, then switch to the next partition
            }

            // ── Step 3: advance to the next partition in the worker's slice ──
            // This is what makes workers "switch between batches": after every
            // batch the cursor moves forward so the next assigned partition gets
            // serviced on the following iteration.
            cursor++;
        }
    }
    catch (OperationCanceledException) { /* normal shutdown */ }
});

Task[] workerTasks = workerPartitions.Select(StartWorker).ToArray();

// ── Live progress display ──────────────────────────────────────────────────────
var displayTask = Task.Run(async () =>
{
    try
    {
        while (!ct.IsCancellationRequested)
        {
            Console.Write(
                $"\r  Sent   orders: {ordersSent,7:N0}  alerts: {prioritySent,5:N0}  │" +
                $"  Processed  orders: {ordersProcessed,7:N0}  alerts: {priorityProcessed,5:N0}   ");
            await Task.Delay(250, ct);
        }
    }
    catch (OperationCanceledException) { }
});

// ── Run until the demo timer fires ────────────────────────────────────────────
try
{
    await Task.WhenAll(workerTasks.Append(producerTask).Append(displayTask));
}
catch (OperationCanceledException) { }
catch (AggregateException ae) when (ae.InnerExceptions.All(e => e is OperationCanceledException)) { }

Console.WriteLine();
Console.WriteLine();
Console.WriteLine("══════════════════════════════════════════════════════════════════════");
Console.WriteLine(" Results");
Console.WriteLine("══════════════════════════════════════════════════════════════════════");
Console.WriteLine($"  Orders  – sent {ordersSent,8:N0}   processed {ordersProcessed,8:N0}");
Console.WriteLine($"  Alerts  – sent {prioritySent,8:N0}   processed {priorityProcessed,8:N0}");
Console.WriteLine();
Console.WriteLine($"  Partition strategy : CsvListPartitionStrategy ({TotalCustomers:N0} customer IDs)");
Console.WriteLine($"  Partitions         : {PartitionCount:N0}  ({PartitionCount} physical sub-queues under \"{OrdersQueue}\")");
Console.WriteLine($"  Workers            : {workerPartitions.Length}  × ~{partitionsPerWorker} partitions each");
Console.WriteLine($"  Non-partitioned    : \"{PriorityQueue}\"  (same IPartitionedQueue instance)");
Console.WriteLine("══════════════════════════════════════════════════════════════════════");

// ── Cleanup ───────────────────────────────────────────────────────────────────
queue.Dispose();
try { Directory.Delete(dbPath, recursive: true); } catch { /* best effort */ }

Console.WriteLine();
Console.WriteLine("  Done. Press any key to exit.");
try { Console.ReadKey(intercept: true); }
catch (InvalidOperationException) { /* non-interactive environment – just exit */ }
