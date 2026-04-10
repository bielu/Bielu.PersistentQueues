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
    /// Enqueues the pre-generated messages into the "test" queue, then receives messages and acknowledges each until the configured MessageCount has been processed.
    /// </summary>
    [Benchmark(Description = "Receive and acknowledge messages")]
    public async Task ReceiveAndAcknowledge()
    {
        // First enqueue all messages
        foreach (var message in _messages!)
        {
            _queue!.Enqueue(message);
        }

        // Then receive and acknowledge
        var count = 0;
        await foreach (var ctx in _queue.Receive("test", cancellationToken: _cts!.Token))
        {
            ctx.QueueContext.SuccessfullyReceived();
            ctx.QueueContext.CommitChanges();
            if (++count >= MessageCount)
                break;
        }
    }

    /// <summary>
    /// Enqueues the pre-generated messages and processes them by receiving batches (up to 10 messages), marking each batch as successfully received and committing changes until MessageCount messages have been processed.
    /// </summary>
    [Benchmark(Description = "Batch receive and acknowledge")]
    public async Task BatchReceiveAndAcknowledge()
    {
        // First enqueue all messages
        foreach (var message in _messages!)
        {
            _queue!.Enqueue(message);
        }

        // Then receive in batches of 10
        var received = 0;
        await foreach (var batch in _queue.ReceiveBatch("test", maxMessages: 10, cancellationToken: _cts!.Token))
        {
            batch.SuccessfullyReceived();
            batch.CommitChanges();
            received += batch.Messages.Length;
            if (received >= MessageCount)
                break;
        }
    }

    /// <summary>
    /// Enqueues the prepared messages, receives them, and schedules each message to be retried later before committing, simulating a retry pattern.
    /// </summary>
    /// <remarks>
    /// Each received message is marked with ReceiveLater(100ms) and then committed. Processing stops after <see cref="MessageCount"/> messages have been handled.
    /// </remarks>
    [Benchmark(Description = "ReceiveLater (retry pattern)")]
    public async Task ReceiveLater()
    {
        // Enqueue messages
        foreach (var message in _messages!)
        {
            _queue!.Enqueue(message);
        }

        // Receive and delay (simulates retry)
        var count = 0;
        await foreach (var ctx in _queue.Receive("test", cancellationToken: _cts!.Token))
        {
            ctx.QueueContext.ReceiveLater(TimeSpan.FromMilliseconds(100));
            ctx.QueueContext.CommitChanges();
            if (++count >= MessageCount)
                break;
        }
    }

    /// <summary>
    /// Moves a batch of pre-generated messages from the "test" queue into a newly created "target" queue.
    /// </summary>
    /// <remarks>
    /// The method creates the "target" queue, enqueues the pre-generated messages into "test", then consumes messages from "test" and moves each to "target" until <see cref="MessageCount"/> messages have been moved and committed.
    /// </remarks>
    /// <returns>A task that completes when <see cref="MessageCount"/> messages have been moved to the "target" queue and the changes have been committed.</returns>
    [Benchmark(Description = "MoveTo (queue transfer)")]
    public async Task MoveTo()
    {
        _queue!.CreateQueue("target");

        // Enqueue messages
        foreach (var message in _messages!)
        {
            _queue.Enqueue(message);
        }

        // Move to another queue
        var count = 0;
        await foreach (var ctx in _queue.Receive("test", cancellationToken: _cts!.Token))
        {
            ctx.QueueContext.MoveTo("target");
            ctx.QueueContext.CommitChanges();
            if (++count >= MessageCount)
                break;
        }
    }

    /// <summary>
    /// Processes messages from the "test" queue in batches and performs mixed operations per batch:
    /// schedules the first message for later delivery, moves the second message to the "target" queue,
    /// marks the batch as successfully received, and commits changes until MessageCount messages are handled.
    /// </summary>
    /// <returns>Completion of the benchmarked batch processing.</returns>
    [Benchmark(Description = "Batch mixed operations")]
    public async Task BatchMixedOperations()
    {
        _queue!.CreateQueue("target");

        // Enqueue messages
        foreach (var message in _messages!)
        {
            _queue.Enqueue(message);
        }

        // Process with mixed operations
        var received = 0;
        await foreach (var batch in _queue.ReceiveBatch("test", maxMessages: 10, cancellationToken: _cts!.Token))
        {
            var messages = batch.Messages;
            if (messages.Length > 0)
            {
                // Simulate: first message -> retry, second -> move, rest -> success
                if (messages.Length > 0)
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
