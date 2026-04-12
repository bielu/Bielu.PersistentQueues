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

            var p1 = partitioned.ResolvePartition(msg1);
            var p2 = partitioned.ResolvePartition(msg2);
            p1.ShouldBe(p2);

            partitioned.Enqueue(msg1);
            partitioned.Enqueue(msg2);

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
            partitioned.EnqueueToPartition(msg, 2);

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
                partitioned.EnqueueToPartition(msg, 0);
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
    public void queues_collapses_partitions_into_single_logical_queue()
    {
        PartitionedStorageScenario(store =>
        {
            var config = new QueueConfiguration()
                .WithDefaultsForTest(Output)
                .StoreMessagesWith(() => store);
            var innerQueue = config.BuildQueue();
            var partitioned = new PartitionedQueue(innerQueue, new HashPartitionStrategy());

            partitioned.CreatePartitionedQueue("orders", 4);

            // The logical view should show "test" (from PartitionedStorageScenario) and "orders"
            // but NOT the individual partition sub-queues
            var queues = partitioned.Queues;
            queues.ShouldContain("test");
            queues.ShouldContain("orders");
            queues.ShouldNotContain("orders:partition-0");
            queues.ShouldNotContain("orders:partition-1");
            queues.ShouldNotContain("orders:partition-2");
            queues.ShouldNotContain("orders:partition-3");
        });
    }

    [Fact]
    public void queues_includes_non_partitioned_queues()
    {
        PartitionedStorageScenario(store =>
        {
            var config = new QueueConfiguration()
                .WithDefaultsForTest(Output)
                .StoreMessagesWith(() => store);
            var innerQueue = config.BuildQueue();
            var partitioned = new PartitionedQueue(innerQueue, new HashPartitionStrategy());

            innerQueue.CreateQueue("plain-queue");
            partitioned.CreatePartitionedQueue("orders", 2);

            var queues = partitioned.Queues;
            queues.ShouldContain("test"); // from scenario
            queues.ShouldContain("plain-queue");
            queues.ShouldContain("orders"); // collapsed from partition sub-queues
            queues.Length.ShouldBe(3);
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
            var p1 = partitioned.ResolvePartition(msg);
            var p2 = partitioned.ResolvePartition(msg);

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

            Should.Throw<ArgumentOutOfRangeException>(() => partitioned.EnqueueToPartition(msg, 5));
            Should.Throw<ArgumentOutOfRangeException>(() => partitioned.EnqueueToPartition(msg, -1));
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
                partitioned.EnqueueToPartition(msg, 0);
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

    [Fact]
    public void get_message_count_returns_zero_for_empty_queue()
    {
        PartitionedStorageScenario(store =>
        {
            store.GetMessageCount("test").ShouldBe(0);
        });
    }

    [Fact]
    public void get_message_count_returns_correct_count_after_enqueue()
    {
        PartitionedStorageScenario(store =>
        {
            var messages = Enumerable.Range(0, 5)
                .Select(i => Message.Create(data: Encoding.UTF8.GetBytes($"msg-{i}"), queue: "test"))
                .ToList();
            store.StoreIncoming(messages);

            store.GetMessageCount("test").ShouldBe(5);
        });
    }

    [Fact]
    public void get_message_count_updates_after_removal()
    {
        PartitionedStorageScenario(store =>
        {
            var messages = Enumerable.Range(0, 3)
                .Select(i => Message.Create(data: Encoding.UTF8.GetBytes($"msg-{i}"), queue: "test"))
                .ToList();
            store.StoreIncoming(messages);
            store.GetMessageCount("test").ShouldBe(3);

            using var tx = store.BeginTransaction();
            store.SuccessfullyReceived(tx, messages[0]);
            tx.Commit();

            store.GetMessageCount("test").ShouldBe(2);
        });
    }

    [Fact]
    public void get_partition_message_count_returns_zero_for_empty_partition()
    {
        PartitionedStorageScenario(store =>
        {
            var config = new QueueConfiguration()
                .WithDefaultsForTest(Output)
                .StoreMessagesWith(() => store);
            var innerQueue = config.BuildQueue();
            var partitioned = new PartitionedQueue(innerQueue, new HashPartitionStrategy());
            partitioned.CreatePartitionedQueue("orders", 4);

            partitioned.GetPartitionMessageCount("orders", 0).ShouldBe(0);
            partitioned.GetPartitionMessageCount("orders", 1).ShouldBe(0);
            partitioned.GetPartitionMessageCount("orders", 2).ShouldBe(0);
            partitioned.GetPartitionMessageCount("orders", 3).ShouldBe(0);
        });
    }

    [Fact]
    public void get_partition_message_count_returns_correct_count()
    {
        PartitionedStorageScenario(store =>
        {
            var config = new QueueConfiguration()
                .WithDefaultsForTest(Output)
                .StoreMessagesWith(() => store);
            var innerQueue = config.BuildQueue();
            var partitioned = new PartitionedQueue(innerQueue, new HashPartitionStrategy());
            partitioned.CreatePartitionedQueue("orders", 4);

            // Enqueue 3 messages to partition 1
            for (int i = 0; i < 3; i++)
            {
                var msg = Message.Create(data: Encoding.UTF8.GetBytes($"msg-{i}"), queue: "orders");
                partitioned.EnqueueToPartition(msg, 1);
            }

            partitioned.GetPartitionMessageCount("orders", 0).ShouldBe(0);
            partitioned.GetPartitionMessageCount("orders", 1).ShouldBe(3);
            partitioned.GetPartitionMessageCount("orders", 2).ShouldBe(0);
            partitioned.GetPartitionMessageCount("orders", 3).ShouldBe(0);
        });
    }

    [Fact]
    public void get_partition_message_count_out_of_range_throws()
    {
        PartitionedStorageScenario(store =>
        {
            var config = new QueueConfiguration()
                .WithDefaultsForTest(Output)
                .StoreMessagesWith(() => store);
            var innerQueue = config.BuildQueue();
            var partitioned = new PartitionedQueue(innerQueue, new HashPartitionStrategy());
            partitioned.CreatePartitionedQueue("orders", 4);

            Should.Throw<ArgumentOutOfRangeException>(() => partitioned.GetPartitionMessageCount("orders", 5));
            Should.Throw<ArgumentOutOfRangeException>(() => partitioned.GetPartitionMessageCount("orders", -1));
        });
    }

    [Fact]
    public void get_active_partitions_returns_empty_when_no_messages()
    {
        PartitionedStorageScenario(store =>
        {
            var config = new QueueConfiguration()
                .WithDefaultsForTest(Output)
                .StoreMessagesWith(() => store);
            var innerQueue = config.BuildQueue();
            var partitioned = new PartitionedQueue(innerQueue, new HashPartitionStrategy());
            partitioned.CreatePartitionedQueue("orders", 4);

            partitioned.GetActivePartitions("orders").ShouldBeEmpty();
        });
    }

    [Fact]
    public void get_active_partitions_returns_only_non_empty_partitions()
    {
        PartitionedStorageScenario(store =>
        {
            var config = new QueueConfiguration()
                .WithDefaultsForTest(Output)
                .StoreMessagesWith(() => store);
            var innerQueue = config.BuildQueue();
            var partitioned = new PartitionedQueue(innerQueue, new HashPartitionStrategy());
            partitioned.CreatePartitionedQueue("orders", 4);

            // Enqueue messages to partitions 1 and 3 only
            var msg1 = Message.Create(data: Encoding.UTF8.GetBytes("a"), queue: "orders");
            partitioned.EnqueueToPartition(msg1, 1);
            var msg2 = Message.Create(data: Encoding.UTF8.GetBytes("b"), queue: "orders");
            partitioned.EnqueueToPartition(msg2, 3);

            var active = partitioned.GetActivePartitions("orders");
            active.ShouldBe(new[] { 1, 3 });
        });
    }

    [Fact]
    public void get_active_partitions_returns_empty_for_unknown_queue()
    {
        PartitionedStorageScenario(store =>
        {
            var config = new QueueConfiguration()
                .WithDefaultsForTest(Output)
                .StoreMessagesWith(() => store);
            var innerQueue = config.BuildQueue();
            var partitioned = new PartitionedQueue(innerQueue, new HashPartitionStrategy());

            // Queue "nope" was never created — should get empty array, not an exception
            partitioned.GetActivePartitions("nope").ShouldBeEmpty();
        });
    }

    [Fact]
    public void get_available_partitions_returns_non_empty_unlocked_partitions()
    {
        PartitionedStorageScenario(store =>
        {
            var config = new QueueConfiguration()
                .WithDefaultsForTest(Output)
                .StoreMessagesWith(() => store);
            var innerQueue = config.BuildQueue();
            var partitioned = new PartitionedQueue(innerQueue, new HashPartitionStrategy());
            partitioned.CreatePartitionedQueue("orders", 4);

            // Enqueue to partitions 0, 1, 2
            for (int p = 0; p < 3; p++)
            {
                var msg = Message.Create(data: Encoding.UTF8.GetBytes($"msg-{p}"), queue: "orders");
                partitioned.EnqueueToPartition(msg, p);
            }

            // Without any consumer, all non-empty partitions should be available
            var available = partitioned.GetAvailablePartitions("orders");
            available.ShouldBe(new[] { 0, 1, 2 });
        });
    }

    [Fact]
    public async Task get_available_partitions_excludes_locked_partitions()
    {
        await PartitionedQueueScenario(async (queue, token) =>
        {
            var partitioned = new PartitionedQueue(queue, new HashPartitionStrategy());
            partitioned.CreatePartitionedQueue("orders", 4);

            // Enqueue to partitions 0 and 1
            for (int p = 0; p < 2; p++)
            {
                var msg = Message.Create(data: Encoding.UTF8.GetBytes($"msg-{p}"), queue: "orders");
                partitioned.EnqueueToPartition(msg, p);
            }

            // Start a consumer on partition 0 to hold the lock
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(token);
            var consumerStarted = new TaskCompletionSource();

            var consumerTask = Task.Run(async () =>
            {
                await foreach (var ctx in partitioned.ReceiveFromPartition("orders", 0, 50, cts.Token))
                {
                    consumerStarted.TrySetResult();
                    // Hold the lock while we wait — don't process
                    try { await Task.Delay(Timeout.Infinite, cts.Token); }
                    catch (OperationCanceledException) { }
                    break;
                }
            }, cts.Token);

            // Wait until the consumer has acquired the partition lock
            await consumerStarted.Task.WaitAsync(TimeSpan.FromSeconds(3));

            // Partition 0 is locked, partition 1 has messages, partitions 2+3 are empty
            var available = partitioned.GetAvailablePartitions("orders");
            available.ShouldBe(new[] { 1 });

            // Also verify GetActivePartitions still sees partition 0 (it has messages, just locked)
            var active = partitioned.GetActivePartitions("orders");
            active.ShouldBe(new[] { 0, 1 });

            await cts.CancelAsync();
            try { await consumerTask; }
            catch (OperationCanceledException) { }
        }, TimeSpan.FromSeconds(10));
    }

    [Fact]
    public void enable_partitioning_converts_non_partitioned_queue()
    {
        PartitionedStorageScenario(store =>
        {
            var config = new QueueConfiguration()
                .WithDefaultsForTest(Output)
                .StoreMessagesWith(() => store);
            var innerQueue = config.BuildQueue();
            var partitioned = new PartitionedQueue(innerQueue, new HashPartitionStrategy());

            // Enqueue messages to the non-partitioned "test" queue
            for (int i = 0; i < 5; i++)
            {
                var msg = Message.Create(data: Encoding.UTF8.GetBytes($"msg-{i}"), queue: "test",
                    partitionKey: $"key-{i}");
                innerQueue.Enqueue(msg);
            }

            store.GetMessageCount("test").ShouldBe(5);

            // Enable partitioning with 3 partitions
            partitioned.EnablePartitioning("test", 3);

            // Base queue should now be empty — messages moved to partitions
            store.GetMessageCount("test").ShouldBe(0);

            // All 5 messages should be distributed across partitions
            long total = 0;
            for (int i = 0; i < 3; i++)
                total += partitioned.GetPartitionMessageCount("test", i);
            total.ShouldBe(5);

            // Partition count should reflect the new layout
            partitioned.GetPartitionCount("test").ShouldBe(3);
        });
    }

    [Fact]
    public void enable_partitioning_on_empty_queue()
    {
        PartitionedStorageScenario(store =>
        {
            var config = new QueueConfiguration()
                .WithDefaultsForTest(Output)
                .StoreMessagesWith(() => store);
            var innerQueue = config.BuildQueue();
            var partitioned = new PartitionedQueue(innerQueue, new HashPartitionStrategy());

            // Enable partitioning on an empty queue
            partitioned.EnablePartitioning("test", 4);

            partitioned.GetPartitionCount("test").ShouldBe(4);

            // All partitions should be empty
            for (int i = 0; i < 4; i++)
                partitioned.GetPartitionMessageCount("test", i).ShouldBe(0);
        });
    }

    [Fact]
    public void enable_partitioning_throws_if_already_partitioned()
    {
        PartitionedStorageScenario(store =>
        {
            var config = new QueueConfiguration()
                .WithDefaultsForTest(Output)
                .StoreMessagesWith(() => store);
            var innerQueue = config.BuildQueue();
            var partitioned = new PartitionedQueue(innerQueue, new HashPartitionStrategy());

            partitioned.CreatePartitionedQueue("test", 4);

            Should.Throw<InvalidOperationException>(() =>
                partitioned.EnablePartitioning("test", 2));
        });
    }

    [Fact]
    public void enable_partitioning_throws_for_invalid_count()
    {
        PartitionedStorageScenario(store =>
        {
            var config = new QueueConfiguration()
                .WithDefaultsForTest(Output)
                .StoreMessagesWith(() => store);
            var innerQueue = config.BuildQueue();
            var partitioned = new PartitionedQueue(innerQueue, new HashPartitionStrategy());

            Should.Throw<ArgumentOutOfRangeException>(() =>
                partitioned.EnablePartitioning("test", 0));
            Should.Throw<ArgumentOutOfRangeException>(() =>
                partitioned.EnablePartitioning("test", -1));
        });
    }

    [Fact]
    public void disable_partitioning_converts_partitioned_to_non_partitioned()
    {
        PartitionedStorageScenario(store =>
        {
            var config = new QueueConfiguration()
                .WithDefaultsForTest(Output)
                .StoreMessagesWith(() => store);
            var innerQueue = config.BuildQueue();
            var partitioned = new PartitionedQueue(innerQueue, new HashPartitionStrategy());

            partitioned.CreatePartitionedQueue("test", 3);

            // Enqueue messages to various partitions
            for (int i = 0; i < 6; i++)
            {
                var msg = Message.Create(data: Encoding.UTF8.GetBytes($"msg-{i}"), queue: "test");
                partitioned.EnqueueToPartition(msg, i % 3);
            }

            // Verify messages are distributed
            long totalBefore = 0;
            for (int i = 0; i < 3; i++)
                totalBefore += partitioned.GetPartitionMessageCount("test", i);
            totalBefore.ShouldBe(6);

            // Disable partitioning
            partitioned.DisablePartitioning("test");

            // Base queue should now have all messages
            store.GetMessageCount("test").ShouldBe(6);

            // Queue is no longer partitioned
            partitioned.GetPartitionCount("test").ShouldBe(0);
        });
    }

    [Fact]
    public void disable_partitioning_throws_if_not_partitioned()
    {
        PartitionedStorageScenario(store =>
        {
            var config = new QueueConfiguration()
                .WithDefaultsForTest(Output)
                .StoreMessagesWith(() => store);
            var innerQueue = config.BuildQueue();
            var partitioned = new PartitionedQueue(innerQueue, new HashPartitionStrategy());

            Should.Throw<InvalidOperationException>(() =>
                partitioned.DisablePartitioning("test"));
        });
    }

    [Fact]
    public void repartition_changes_partition_count()
    {
        PartitionedStorageScenario(store =>
        {
            var config = new QueueConfiguration()
                .WithDefaultsForTest(Output)
                .StoreMessagesWith(() => store);
            var innerQueue = config.BuildQueue();
            var partitioned = new PartitionedQueue(innerQueue, new HashPartitionStrategy());

            partitioned.CreatePartitionedQueue("test", 2);

            // Enqueue messages to both partitions
            for (int i = 0; i < 10; i++)
            {
                var msg = Message.Create(data: Encoding.UTF8.GetBytes($"msg-{i}"), queue: "test",
                    partitionKey: $"key-{i}");
                partitioned.EnqueueToPartition(msg, i % 2);
            }

            // Verify all 10 messages exist
            long totalBefore = 0;
            for (int i = 0; i < 2; i++)
                totalBefore += partitioned.GetPartitionMessageCount("test", i);
            totalBefore.ShouldBe(10);

            // Repartition from 2 to 4
            partitioned.Repartition("test", 4);

            // Partition count should be updated
            partitioned.GetPartitionCount("test").ShouldBe(4);

            // All 10 messages should still exist across the new partitions
            long totalAfter = 0;
            for (int i = 0; i < 4; i++)
                totalAfter += partitioned.GetPartitionMessageCount("test", i);
            totalAfter.ShouldBe(10);
        });
    }

    [Fact]
    public void repartition_same_count_is_noop()
    {
        PartitionedStorageScenario(store =>
        {
            var config = new QueueConfiguration()
                .WithDefaultsForTest(Output)
                .StoreMessagesWith(() => store);
            var innerQueue = config.BuildQueue();
            var partitioned = new PartitionedQueue(innerQueue, new HashPartitionStrategy());

            partitioned.CreatePartitionedQueue("test", 4);

            // Enqueue some messages
            for (int i = 0; i < 4; i++)
            {
                var msg = Message.Create(data: Encoding.UTF8.GetBytes($"msg-{i}"), queue: "test");
                partitioned.EnqueueToPartition(msg, i);
            }

            // Repartition with the same count should be a no-op
            partitioned.Repartition("test", 4);

            partitioned.GetPartitionCount("test").ShouldBe(4);

            // Messages should remain in their partitions unchanged
            for (int i = 0; i < 4; i++)
                partitioned.GetPartitionMessageCount("test", i).ShouldBe(1);
        });
    }

    [Fact]
    public void repartition_throws_if_not_partitioned()
    {
        PartitionedStorageScenario(store =>
        {
            var config = new QueueConfiguration()
                .WithDefaultsForTest(Output)
                .StoreMessagesWith(() => store);
            var innerQueue = config.BuildQueue();
            var partitioned = new PartitionedQueue(innerQueue, new HashPartitionStrategy());

            Should.Throw<InvalidOperationException>(() =>
                partitioned.Repartition("test", 4));
        });
    }

    [Fact]
    public void repartition_throws_for_invalid_count()
    {
        PartitionedStorageScenario(store =>
        {
            var config = new QueueConfiguration()
                .WithDefaultsForTest(Output)
                .StoreMessagesWith(() => store);
            var innerQueue = config.BuildQueue();
            var partitioned = new PartitionedQueue(innerQueue, new HashPartitionStrategy());

            partitioned.CreatePartitionedQueue("test", 2);

            Should.Throw<ArgumentOutOfRangeException>(() =>
                partitioned.Repartition("test", 0));
            Should.Throw<ArgumentOutOfRangeException>(() =>
                partitioned.Repartition("test", -1));
        });
    }

    [Fact]
    public void repartition_shrink_preserves_all_messages()
    {
        PartitionedStorageScenario(store =>
        {
            var config = new QueueConfiguration()
                .WithDefaultsForTest(Output)
                .StoreMessagesWith(() => store);
            var innerQueue = config.BuildQueue();
            var partitioned = new PartitionedQueue(innerQueue, new HashPartitionStrategy());

            partitioned.CreatePartitionedQueue("test", 4);

            // Enqueue messages to all 4 partitions
            for (int i = 0; i < 8; i++)
            {
                var msg = Message.Create(data: Encoding.UTF8.GetBytes($"msg-{i}"), queue: "test",
                    partitionKey: $"key-{i}");
                partitioned.EnqueueToPartition(msg, i % 4);
            }

            // Repartition from 4 to 2
            partitioned.Repartition("test", 2);

            partitioned.GetPartitionCount("test").ShouldBe(2);

            // All 8 messages should still exist
            long total = 0;
            for (int i = 0; i < 2; i++)
                total += partitioned.GetPartitionMessageCount("test", i);
            total.ShouldBe(8);
        });
    }

    [Fact]
    public void enable_then_disable_roundtrip()
    {
        PartitionedStorageScenario(store =>
        {
            var config = new QueueConfiguration()
                .WithDefaultsForTest(Output)
                .StoreMessagesWith(() => store);
            var innerQueue = config.BuildQueue();
            var partitioned = new PartitionedQueue(innerQueue, new HashPartitionStrategy());

            // Enqueue messages to non-partitioned queue
            for (int i = 0; i < 5; i++)
            {
                var msg = Message.Create(data: Encoding.UTF8.GetBytes($"msg-{i}"), queue: "test");
                innerQueue.Enqueue(msg);
            }

            // Convert to partitioned
            partitioned.EnablePartitioning("test", 3);
            store.GetMessageCount("test").ShouldBe(0);
            partitioned.GetPartitionCount("test").ShouldBe(3);

            // Convert back to non-partitioned
            partitioned.DisablePartitioning("test");
            store.GetMessageCount("test").ShouldBe(5);
            partitioned.GetPartitionCount("test").ShouldBe(0);
        });
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
