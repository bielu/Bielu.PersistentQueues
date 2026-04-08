using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Bielu.PersistentQueues.Partitioning;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace Bielu.PersistentQueues.Examples.Services;

/// <summary>
/// Manages a dynamic pool of partition-consumer tasks whose size is governed by
/// <see cref="WorkerPoolOptions.WorkerCount"/>. Changing that value in
/// <c>appsettings.json</c> rescales the pool at runtime — no restart required.
///
/// Only one worker may hold the <see cref="BatchProcessingLock"/> at a time,
/// so batch commits are always serialised across the entire pool.
/// </summary>
internal sealed class PartitionCoordinatorService(
    IPartitionedQueue queue,
    int totalPartitions,
    DemoStats stats,
    BatchProcessingLock batchLock,
    IOptionsMonitor<WorkerPoolOptions> workerOptions) : BackgroundService
{
    private const string OrdersQueue = "orders";

    // Serialises concurrent rescale requests (e.g. rapid file-save events).
    private readonly SemaphoreSlim _scaleLock = new(1, 1);

    private CancellationTokenSource? _workersCts;
    private Task[] _workerTasks = [];

    // ── Lifecycle ─────────────────────────────────────────────────────────────

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        StartWorkers(Math.Max(1, workerOptions.CurrentValue.WorkerCount), stoppingToken);

        // React to live config changes (e.g. edit WorkerPool:WorkerCount in appsettings.json).
        using var changeReg = workerOptions.OnChange(opts =>
        {
            if (!stoppingToken.IsCancellationRequested)
            {
                var rescaleTask = RescaleAsync(Math.Max(1, opts.WorkerCount), stoppingToken);
                rescaleTask.ContinueWith(
                    t => Console.WriteLine($"\n  [Coordinator] Rescale error: {t.Exception?.GetBaseException().Message}"),
                    CancellationToken.None,
                    TaskContinuationOptions.OnlyOnFaulted,
                    TaskScheduler.Default);
            }
        });

        // Park here until the host requests shutdown.
        try { await Task.Delay(Timeout.Infinite, stoppingToken); }
        catch (OperationCanceledException) { }

        // Drain all worker tasks cleanly.
        if (_workersCts is not null)
        {
            await _workersCts.CancelAsync();
            try { await Task.WhenAll(_workerTasks); }
            catch (OperationCanceledException) { }
            _workersCts.Dispose();
            _workersCts = null;
        }
    }

    public override void Dispose()
    {
        _scaleLock.Dispose();
        base.Dispose();
    }

    // ── Internal helpers ──────────────────────────────────────────────────────

    /// <summary>Starts a fresh set of worker tasks for the given count.</summary>
    private void StartWorkers(int workerCount, CancellationToken stoppingToken)
    {
        _workersCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        var ct = _workersCts.Token;

        int perWorker = (int)Math.Ceiling((double)totalPartitions / workerCount);

        _workerTasks = Enumerable.Range(0, workerCount)
            .Select(w =>
            {
                int[] slice = Enumerable.Range(w * perWorker, perWorker)
                    .Where(p => p < totalPartitions)
                    .ToArray();
                return slice.Length > 0 ? RunWorkerAsync(slice, ct) : Task.CompletedTask;
            })
            .ToArray();

        Console.WriteLine(
            $"\n  [Coordinator] {workerCount} worker(s) active " +
            $"(~{perWorker} partitions each out of {totalPartitions} total).");
    }

    /// <summary>
    /// Stops all current workers and starts a new pool with <paramref name="newCount"/> workers.
    /// Serialised via <see cref="_scaleLock"/> to prevent overlapping rescales.
    /// </summary>
    private async Task RescaleAsync(int newCount, CancellationToken stoppingToken)
    {
        if (stoppingToken.IsCancellationRequested) return;

        bool lockAcquired = false;
        try
        {
            await _scaleLock.WaitAsync(stoppingToken);
            lockAcquired = true;

            Console.WriteLine($"\n  [Coordinator] Rescaling to {newCount} worker(s)…");

            // Stop current pool.
            if (_workersCts is not null)
            {
                await _workersCts.CancelAsync();
                try { await Task.WhenAll(_workerTasks); }
                catch (OperationCanceledException) { }
                _workersCts.Dispose();
                _workersCts = null;
            }

            if (!stoppingToken.IsCancellationRequested)
                StartWorkers(newCount, stoppingToken);
        }
        catch (OperationCanceledException) { }
        finally
        {
            if (lockAcquired) _scaleLock.Release();
        }
    }

    // ── Worker loop ───────────────────────────────────────────────────────────

    private async Task RunWorkerAsync(int[] partitions, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            // Ask the queue which of our assigned partitions are available
            // (have messages AND are not locked by another consumer).
            var available = queue.GetAvailablePartitions(OrdersQueue)
                .Where(p => partitions.Contains(p))
                .ToArray();

            if (available.Length == 0)
            {
                // No work right now — sleep briefly and re-check.
                try { await Task.Delay(10, ct); }
                catch (OperationCanceledException) { break; }
                continue;
            }

            foreach (var partition in available)
            {
                if (ct.IsCancellationRequested) break;

                bool lockAcquired = false;
                try
                {
                    await batchLock.WaitAsync(ct);
                    lockAcquired = true;

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
                            stats.AddOrdersProcessed(batch.Messages.Length);
                            batch.SuccessfullyReceived();
                        }
                        break; // one batch per partition per rotation
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
}
