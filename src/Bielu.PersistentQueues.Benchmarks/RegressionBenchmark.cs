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

    [Benchmark(Description = "Enqueue messages")]
    public void Enqueue()
    {
        foreach (var message in _messages!)
        {
            _queue!.Enqueue(message);
        }
    }

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
