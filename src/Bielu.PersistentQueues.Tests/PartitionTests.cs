using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Bielu.PersistentQueues.Partitioning;
using Bielu.PersistentQueues.Serialization;
using Bielu.PersistentQueues.Storage.LMDB;
using LightningDB;
using Shouldly;
using Xunit;
using Xunit.Abstractions;

namespace Bielu.PersistentQueues.Tests;

public class PartitionStrategyTests
{
    [Fact]
    public void hash_strategy_same_key_same_partition()
    {
        var strategy = new HashPartitionStrategy();
        var msg1 = Message.Create(data: Encoding.UTF8.GetBytes("a"), queue: "test", partitionKey: "order-123");
        var msg2 = Message.Create(data: Encoding.UTF8.GetBytes("b"), queue: "test", partitionKey: "order-123");

        var p1 = strategy.GetPartition(msg1, 8);
        var p2 = strategy.GetPartition(msg2, 8);

        p1.ShouldBe(p2);
    }

    [Fact]
    public void hash_strategy_different_keys_distribute_across_partitions()
    {
        var strategy = new HashPartitionStrategy();
        var partitions = new HashSet<int>();

        for (int i = 0; i < 100; i++)
        {
            var msg = Message.Create(data: Encoding.UTF8.GetBytes("data"), queue: "test",
                partitionKey: $"key-{i}");
            partitions.Add(strategy.GetPartition(msg, 8));
        }

        // With 100 different keys and 8 partitions, we should hit most partitions
        partitions.Count.ShouldBeGreaterThan(1);
    }

    [Fact]
    public void hash_strategy_no_key_uses_message_id()
    {
        var strategy = new HashPartitionStrategy();
        var partitions = new HashSet<int>();

        for (int i = 0; i < 50; i++)
        {
            var msg = Message.Create(data: Encoding.UTF8.GetBytes("data"), queue: "test");
            partitions.Add(strategy.GetPartition(msg, 8));
        }

        // Should distribute across multiple partitions
        partitions.Count.ShouldBeGreaterThan(1);
    }

    [Fact]
    public void hash_strategy_single_partition_always_returns_zero()
    {
        var strategy = new HashPartitionStrategy();
        var msg = Message.Create(data: Encoding.UTF8.GetBytes("data"), queue: "test", partitionKey: "key");

        strategy.GetPartition(msg, 1).ShouldBe(0);
    }

    [Fact]
    public void hash_strategy_invalid_partition_count_throws()
    {
        var strategy = new HashPartitionStrategy();
        var msg = Message.Create(data: Encoding.UTF8.GetBytes("data"), queue: "test");

        Should.Throw<ArgumentOutOfRangeException>(() => strategy.GetPartition(msg, 0));
        Should.Throw<ArgumentOutOfRangeException>(() => strategy.GetPartition(msg, -1));
    }

    [Fact]
    public void round_robin_distributes_evenly()
    {
        var strategy = new RoundRobinPartitionStrategy();
        var counts = new int[4];

        for (int i = 0; i < 100; i++)
        {
            var msg = Message.Create(data: Encoding.UTF8.GetBytes("data"), queue: "test");
            var partition = strategy.GetPartition(msg, 4);
            counts[partition]++;
        }

        // Round-robin should give exactly 25 each
        counts[0].ShouldBe(25);
        counts[1].ShouldBe(25);
        counts[2].ShouldBe(25);
        counts[3].ShouldBe(25);
    }

    [Fact]
    public void round_robin_single_partition()
    {
        var strategy = new RoundRobinPartitionStrategy();
        var msg = Message.Create(data: Encoding.UTF8.GetBytes("data"), queue: "test");

        strategy.GetPartition(msg, 1).ShouldBe(0);
    }

    [Fact]
    public void explicit_strategy_routes_to_specified_partition()
    {
        var strategy = new ExplicitPartitionStrategy();

        var msg0 = Message.Create(data: Encoding.UTF8.GetBytes("data"), queue: "test", partitionKey: "0");
        var msg1 = Message.Create(data: Encoding.UTF8.GetBytes("data"), queue: "test", partitionKey: "1");
        var msg2 = Message.Create(data: Encoding.UTF8.GetBytes("data"), queue: "test", partitionKey: "2");

        strategy.GetPartition(msg0, 4).ShouldBe(0);
        strategy.GetPartition(msg1, 4).ShouldBe(1);
        strategy.GetPartition(msg2, 4).ShouldBe(2);
    }

    [Fact]
    public void explicit_strategy_wraps_with_modulo()
    {
        var strategy = new ExplicitPartitionStrategy();
        var msg = Message.Create(data: Encoding.UTF8.GetBytes("data"), queue: "test", partitionKey: "7");

        strategy.GetPartition(msg, 4).ShouldBe(3); // 7 % 4 = 3
    }

    [Fact]
    public void explicit_strategy_no_key_returns_zero()
    {
        var strategy = new ExplicitPartitionStrategy();
        var msg = Message.Create(data: Encoding.UTF8.GetBytes("data"), queue: "test");

        strategy.GetPartition(msg, 4).ShouldBe(0);
    }

    [Fact]
    public void explicit_strategy_invalid_key_returns_zero()
    {
        var strategy = new ExplicitPartitionStrategy();
        var msg = Message.Create(data: Encoding.UTF8.GetBytes("data"), queue: "test", partitionKey: "not-a-number");

        strategy.GetPartition(msg, 4).ShouldBe(0);
    }
}

public class PartitionConstantsTests
{
    [Fact]
    public void format_partition_queue_name()
    {
        PartitionConstants.FormatPartitionQueueName("orders", 0).ShouldBe("orders:partition-0");
        PartitionConstants.FormatPartitionQueueName("orders", 3).ShouldBe("orders:partition-3");
    }

    [Fact]
    public void try_parse_partition_queue_name_success()
    {
        PartitionConstants.TryParsePartitionQueueName("orders:partition-2", out var queueName, out var index)
            .ShouldBeTrue();
        queueName.ShouldBe("orders");
        index.ShouldBe(2);
    }

    [Fact]
    public void try_parse_partition_queue_name_not_partitioned()
    {
        PartitionConstants.TryParsePartitionQueueName("orders", out var queueName, out var index)
            .ShouldBeFalse();
        queueName.ShouldBe("orders");
        index.ShouldBe(-1);
    }
}

public class PartitionedQueueTests : TestBase
{
    public PartitionedQueueTests(ITestOutputHelper output)
    {
        Output = output;
    }

    [Fact]
    public async Task enqueue_to_partition_routes_by_key()
    {
        await PartitionedQueueScenario(async (queue, token) =>
        {
            var partitioned = new PartitionedQueue(queue, new HashPartitionStrategy());
            partitioned.CreatePartitionedQueue("orders", 4);

            // Send messages with the same partition key — they should land in the same partition
            var msg1 = Message.Create(data: Encoding.UTF8.GetBytes("order-a"), queue: "orders", partitionKey: "customer-1");
            var msg2 = Message.Create(data: Encoding.UTF8.GetBytes("order-b"), queue: "orders", partitionKey: "customer-1");

            var p1 = partitioned.ResolvePartition(msg1, "orders");
            var p2 = partitioned.ResolvePartition(msg2, "orders");
            p1.ShouldBe(p2);

            partitioned.EnqueueToPartition(msg1, "orders");
            partitioned.EnqueueToPartition(msg2, "orders");

            // Receive from the resolved partition
            var received = new List<Message>();
            await foreach (var ctx in partitioned.ReceiveFromPartition("orders", p1, 50, token))
            {
                received.Add(ctx.Message);
                ctx.QueueContext.SuccessfullyReceived();
                ctx.QueueContext.CommitChanges();
                if (received.Count >= 2) break;
            }

            received.Count.ShouldBe(2);
            received[0].DataArray.ShouldBe(Encoding.UTF8.GetBytes("order-a"));
            received[1].DataArray.ShouldBe(Encoding.UTF8.GetBytes("order-b"));
        }, TimeSpan.FromSeconds(5));
    }

    [Fact]
    public async Task enqueue_to_specific_partition()
    {
        await PartitionedQueueScenario(async (queue, token) =>
        {
            var partitioned = new PartitionedQueue(queue, new HashPartitionStrategy());
            partitioned.CreatePartitionedQueue("events", 3);

            var msg = Message.Create(data: Encoding.UTF8.GetBytes("event-data"), queue: "events");
            partitioned.EnqueueToPartition(msg, "events", 2);

            // Should be retrievable from partition 2
            var received = await partitioned.ReceiveFromPartition("events", 2, 50, token).FirstAsync(token);
            received.Message.DataArray.ShouldBe(Encoding.UTF8.GetBytes("event-data"));
            received.QueueContext.SuccessfullyReceived();
            received.QueueContext.CommitChanges();
        }, TimeSpan.FromSeconds(3));
    }

    [Fact]
    public async Task receive_batch_from_partition()
    {
        await PartitionedQueueScenario(async (queue, token) =>
        {
            var partitioned = new PartitionedQueue(queue, new RoundRobinPartitionStrategy());
            partitioned.CreatePartitionedQueue("batch-q", 2);

            // Enqueue messages to partition 0
            for (int i = 0; i < 5; i++)
            {
                var msg = Message.Create(data: Encoding.UTF8.GetBytes($"msg-{i}"), queue: "batch-q");
                partitioned.EnqueueToPartition(msg, "batch-q", 0);
            }

            await foreach (var batch in partitioned.ReceiveBatchFromPartition("batch-q", 0,
                               maxMessages: 5, pollIntervalInMilliseconds: 50, cancellationToken: token))
            {
                batch.Messages.Length.ShouldBe(5);
                batch.SuccessfullyReceived();
                batch.CommitChanges();
                break;
            }
        }, TimeSpan.FromSeconds(3));
    }

    [Fact]
    public void create_partitioned_queue_creates_all_partitions()
    {
        PartitionedStorageScenario(store =>
        {
            var config = new QueueConfiguration()
                .WithDefaultsForTest(Output)
                .StoreMessagesWith(() => store);
            var innerQueue = config.BuildQueue();
            var partitioned = new PartitionedQueue(innerQueue, new HashPartitionStrategy());

            partitioned.CreatePartitionedQueue("test", 4);

            partitioned.GetPartitionCount("test").ShouldBe(4);

            var queues = innerQueue.Queues;
            queues.ShouldContain("test:partition-0");
            queues.ShouldContain("test:partition-1");
            queues.ShouldContain("test:partition-2");
            queues.ShouldContain("test:partition-3");
        });
    }

    [Fact]
    public void resolve_partition_is_consistent_for_same_key()
    {
        PartitionedStorageScenario(store =>
        {
            var config = new QueueConfiguration()
                .WithDefaultsForTest(Output)
                .StoreMessagesWith(() => store);
            var innerQueue = config.BuildQueue();
            var partitioned = new PartitionedQueue(innerQueue, new HashPartitionStrategy());
            partitioned.CreatePartitionedQueue("test", 4);

            var msg = Message.Create(data: Encoding.UTF8.GetBytes("data"), queue: "test", partitionKey: "key-1");
            var p1 = partitioned.ResolvePartition(msg, "test");
            var p2 = partitioned.ResolvePartition(msg, "test");

            p1.ShouldBe(p2);
            p1.ShouldBeGreaterThanOrEqualTo(0);
            p1.ShouldBeLessThan(4);
        });
    }

    [Fact]
    public void enqueue_to_out_of_range_partition_throws()
    {
        PartitionedStorageScenario(store =>
        {
            var config = new QueueConfiguration()
                .WithDefaultsForTest(Output)
                .StoreMessagesWith(() => store);
            var innerQueue = config.BuildQueue();
            var partitioned = new PartitionedQueue(innerQueue, new HashPartitionStrategy());
            partitioned.CreatePartitionedQueue("test", 4);

            var msg = Message.Create(data: Encoding.UTF8.GetBytes("data"), queue: "test");

            Should.Throw<ArgumentOutOfRangeException>(() => partitioned.EnqueueToPartition(msg, "test", 5));
            Should.Throw<ArgumentOutOfRangeException>(() => partitioned.EnqueueToPartition(msg, "test", -1));
        });
    }

    [Fact]
    public async Task concurrent_receive_from_same_partition_does_not_duplicate()
    {
        await PartitionedQueueScenario(async (queue, token) =>
        {
            var partitioned = new PartitionedQueue(queue, new HashPartitionStrategy());
            partitioned.CreatePartitionedQueue("orders", 2);

            // Enqueue 5 messages to partition 0
            for (int i = 0; i < 5; i++)
            {
                var msg = Message.Create(data: Encoding.UTF8.GetBytes($"msg-{i}"), queue: "orders");
                partitioned.EnqueueToPartition(msg, "orders", 0);
            }

            var allReceived = new ConcurrentBag<string>();

            // Two consumers try to receive from partition 0 concurrently.
            // Per-partition locking ensures they are serialised: the first consumer
            // processes and commits all messages, the second gets an empty batch.
            async Task ConsumeAsync(CancellationToken ct)
            {
                try
                {
                    await foreach (var batch in partitioned.ReceiveBatchFromPartition("orders", 0,
                        maxMessages: 5, batchTimeoutInMilliseconds: 200,
                        pollIntervalInMilliseconds: 50, cancellationToken: ct))
                    {
                        foreach (var msg in batch.Messages)
                            allReceived.Add(Encoding.UTF8.GetString(msg.DataArray!));
                        if (batch.Messages.Length > 0)
                        {
                            batch.SuccessfullyReceived();
                            batch.CommitChanges();
                        }
                        break;
                    }
                }
                catch (OperationCanceledException) { }
            }

            using var cts = CancellationTokenSource.CreateLinkedTokenSource(token);
            cts.CancelAfter(TimeSpan.FromSeconds(5));
            var ct = cts.Token;

            await Task.WhenAll(ConsumeAsync(ct), ConsumeAsync(ct));

            // Each message should be received exactly once (no duplicates from concurrent access)
            allReceived.Count.ShouldBe(5);
            allReceived.Distinct().Count().ShouldBe(5);
        }, TimeSpan.FromSeconds(10));
    }

    /// <summary>
    /// StorageScenario variant with higher MaxDatabases to support partitioned queues.
    /// </summary>
    private void PartitionedStorageScenario(Action<LmdbMessageStore> action)
    {
        using var env = new LightningEnvironment(TempPath(), new EnvironmentConfiguration { MaxDatabases = 20, MapSize = 1024 * 1024 * 100 });
        using var store = new LmdbMessageStore(env, new MessageSerializer());
        store.CreateQueue("test");
        action(store);
    }

    [Fact]
    public void message_partition_key_roundtrip()
    {
        var msg = Message.Create(data: Encoding.UTF8.GetBytes("data"), queue: "test", partitionKey: "my-key");
        msg.PartitionKeyString.ShouldBe("my-key");
        msg.PartitionKey.Span.SequenceEqual("my-key".AsSpan()).ShouldBeTrue();
    }

    [Fact]
    public void message_with_partition_key_preserves_key()
    {
        var msg = Message.Create(data: Encoding.UTF8.GetBytes("data"), queue: "test", partitionKey: "key-1");
        var updated = msg.WithPartitionKey("key-2".AsMemory());

        updated.PartitionKeyString.ShouldBe("key-2");
        updated.DataArray.ShouldBe(Encoding.UTF8.GetBytes("data"));
    }

    [Fact]
    public void message_without_partition_key_is_empty()
    {
        var msg = Message.Create(data: Encoding.UTF8.GetBytes("data"), queue: "test");
        msg.PartitionKey.IsEmpty.ShouldBeTrue();
        msg.PartitionKeyString.ShouldBeNull();
    }

    /// <summary>
    /// QueueScenario variant with higher MaxDatabases to support partitioned queues.
    /// </summary>
    private async Task PartitionedQueueScenario(Func<IQueue, CancellationToken, Task> scenario, TimeSpan timeout, string queueName = "test")
    {
        using var cancellation = new CancellationTokenSource(timeout);
        var serializer = new MessageSerializer();
        using var env = new LightningEnvironment(TempPath(), new EnvironmentConfiguration { MaxDatabases = 20, MapSize = 1024 * 1024 * 100 });
        var queueConfiguration = new QueueConfiguration()
            .WithDefaults()
            .LogWith(new Bielu.PersistentQueues.Logging.RecordingLogger(OutputWriter, Microsoft.Extensions.Logging.LogLevel.Information))
            .SerializeWith(serializer)
            .StoreWithLmdb(() => env);
        using var queue = queueConfiguration.BuildAndStartQueue(queueName);
        await scenario(queue, cancellation.Token);
        await cancellation.CancelAsync();
    }
}
