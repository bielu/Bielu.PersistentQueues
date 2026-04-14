using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Engines;
using LightningDB;
using Bielu.PersistentQueues.Storage.LMDB;

namespace Bielu.PersistentQueues.Benchmarks;

/// <summary>
/// Lightweight regression benchmark suite designed for CI/CD pipelines.
/// Focuses on core operations with small datasets for fast execution.
/// </summary>
[SimpleJob(RunStrategy.Throughput, iterationCount: 5, warmupCount: 1)]
[MemoryDiagnoser]
[MinColumn, MaxColumn, MeanColumn, MedianColumn]
public class RegressionBenchmark
{
    private IQueue? _queue;
    private Message[]? _messages;
    private string _queuePath = null!;
    private CancellationTokenSource? _cts;

    /// <summary>
    /// Small message count for fast CI/CD execution (< 30 seconds)
    /// </summary>
    [Params(100, 1_000)]
    public int MessageCount { get; set; }

    [Params(64, 512)]
    public int MessageDataSize { get; set; }

    /// <summary>
    /// Sets up a temporary LMDB-backed queue environment, creates and starts a "test" queue, pre-generates deterministic test messages, and initializes a cancellation token source.
    /// </summary>
    [GlobalSetup]
    public void GlobalSetup()
    {
        _queuePath = Path.Combine(Path.GetTempPath(), "regression-bench", Guid.NewGuid().ToString());
        var envConfig = new EnvironmentConfiguration
        {
            MapSize = 1024 * 1024 * 100, // 100MB - sufficient for regression tests
            MaxDatabases = 5
        };

        _queue = new QueueConfiguration()
            .WithDefaults()
            .StoreWithLmdb(_queuePath, envConfig)
            .BuildQueue();

        _queue.CreateQueue("test");
        _queue.CreateQueue("target");
        _queue.Start();

        // Pre-generate test messages
        _messages = new Message[MessageCount];
        var random = new Random(42);
        for (var i = 0; i < MessageCount; i++)
        {
            var data = new byte[MessageDataSize];
            random.NextBytes(data);
            _messages[i] = Message.Create(
                data: data,
                queue: "test"
            );
        }

        _cts = new CancellationTokenSource();
    }

    /// <summary>
    /// Cleans up benchmark resources by cancelling and disposing the cancellation token, disposing the queue, and removing the temporary queue directory.
    /// </summary>
    /// <remarks>
    /// If the temporary queue directory exists, it will be deleted recursively.
    /// </remarks>
    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _cts?.Cancel();
        _cts?.Dispose();
        _queue?.Dispose();
        if (Directory.Exists(_queuePath))
        {
            Directory.Delete(_queuePath, true);
        }
    }

    /// <summary>
    /// Enqueues the pre-generated messages into the "test" queue before each iteration of benchmarks
    /// that require a pre-populated queue (receive, move, and batch operations).
    /// </summary>
    [IterationSetup(Targets = new[] { nameof(ReceiveAndAcknowledgeAsync), nameof(BatchReceiveAndAcknowledgeAsync), nameof(ReceiveLaterAsync), nameof(MoveToAsync), nameof(BatchMixedOperationsAsync) })]
    public void SetupQueueMessages()
    {
        foreach (var message in _messages!)
        {
            _queue!.Enqueue(message);
        }
    }

    /// <summary>
    /// Drains remaining messages from both queues after each iteration to prevent state accumulation
    /// between benchmark runs. Waits briefly for any delayed messages (e.g., from ReceiveLater)
    /// to become available before draining.
    /// </summary>
    [IterationCleanup]
    public void CleanupQueueState()
    {
        // Wait for delayed messages (e.g., ReceiveLater with 100ms) to become available
        Thread.Sleep(200);
        DrainQueueSync("test");
        DrainQueueSync("target");
    }

    private void DrainQueueSync(string queueName)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(150));
        Task.Run(async () =>
        {
            try
            {
                await foreach (var ctx in _queue!.Receive(queueName, cancellationToken: cts.Token))
                {
                    ctx.QueueContext.SuccessfullyReceived();
                    ctx.QueueContext.CommitChanges();
                }
            }
            catch (OperationCanceledException) { }
        }).Wait(200);
    }

    /// <summary>
    /// Enqueues all pre-generated messages into the configured benchmark queue.
    /// </summary>
    [Benchmark(Description = "Enqueue messages")]
    public void Enqueue()
    {
        foreach (var message in _messages!)
        {
            _queue!.Enqueue(message);
        }
    }

    /// <summary>
    /// Receives messages from the "test" queue and acknowledges each until the configured MessageCount has been processed.
    /// Messages are pre-enqueued by the iteration setup.
    /// </summary>
    [Benchmark(Description = "Receive and acknowledge messages")]
    public async Task ReceiveAndAcknowledgeAsync()
    {
        var count = 0;
        await foreach (var ctx in _queue!.Receive("test", cancellationToken: _cts!.Token))
        {
            ctx.QueueContext.SuccessfullyReceived();
            ctx.QueueContext.CommitChanges();
            if (++count >= MessageCount)
                break;
        }
    }

    /// <summary>
    /// Receives messages from the "test" queue in batches (up to 10 messages), marking each batch as
    /// successfully received and committing changes until MessageCount messages have been processed.
    /// Messages are pre-enqueued by the iteration setup.
    /// </summary>
    [Benchmark(Description = "Batch receive and acknowledge")]
    public async Task BatchReceiveAndAcknowledgeAsync()
    {
        var received = 0;
        await foreach (var batch in _queue!.ReceiveBatch("test", maxMessages: 10, cancellationToken: _cts!.Token))
        {
            batch.SuccessfullyReceived();
            batch.CommitChanges();
            received += batch.Messages.Length;
            if (received >= MessageCount)
                break;
        }
    }

    /// <summary>
    /// Receives messages from the "test" queue and schedules each for later delivery, simulating a retry pattern.
    /// Messages are pre-enqueued by the iteration setup; delayed messages are drained in iteration cleanup.
    /// </summary>
    [Benchmark(Description = "ReceiveLater (retry pattern)")]
    public async Task ReceiveLaterAsync()
    {
        var count = 0;
        await foreach (var ctx in _queue!.Receive("test", cancellationToken: _cts!.Token))
        {
            ctx.QueueContext.ReceiveLater(TimeSpan.FromMilliseconds(100));
            ctx.QueueContext.CommitChanges();
            if (++count >= MessageCount)
                break;
        }
    }

    /// <summary>
    /// Moves pre-enqueued messages from the "test" queue to the "target" queue.
    /// The "target" queue is created once in global setup; messages are pre-enqueued by the iteration setup.
    /// </summary>
    [Benchmark(Description = "MoveTo (queue transfer)")]
    public async Task MoveToAsync()
    {
        var count = 0;
        await foreach (var ctx in _queue!.Receive("test", cancellationToken: _cts!.Token))
        {
            ctx.QueueContext.MoveTo("target");
            ctx.QueueContext.CommitChanges();
            if (++count >= MessageCount)
                break;
        }
    }

    /// <summary>
    /// Processes pre-enqueued messages from the "test" queue in batches with mixed operations:
    /// schedules the first message for later delivery, moves the second message to the "target" queue,
    /// marks the batch as successfully received, and commits changes until MessageCount messages are handled.
    /// </summary>
    [Benchmark(Description = "Batch mixed operations")]
    public async Task BatchMixedOperationsAsync()
    {
        var received = 0;
        await foreach (var batch in _queue!.ReceiveBatch("test", maxMessages: 10, cancellationToken: _cts!.Token))
        {
            var messages = batch.Messages;
            if (messages.Length > 0)
            {
                // Simulate: first message -> retry, second -> move, rest -> success
                batch.ReceiveLater(new[] { messages[0].Id.MessageIdentifier }, TimeSpan.FromMilliseconds(100));
                if (messages.Length > 1)
                    batch.MoveTo("target", new[] { messages[1] });
                // Rest are automatically handled by SuccessfullyReceived
                batch.SuccessfullyReceived();
            }
            batch.CommitChanges();

            received += messages.Length;
            if (received >= MessageCount)
                break;
        }
    }
}
