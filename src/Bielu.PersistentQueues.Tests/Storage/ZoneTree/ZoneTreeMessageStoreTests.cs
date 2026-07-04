using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Bielu.PersistentQueues.Serialization;
using Bielu.PersistentQueues.Storage;
using Bielu.PersistentQueues.Storage.ZoneTree;
using Bielu.PersistentQueues.Tests.Storage.Shared;
using Shouldly;
using ZoneTree;
using ZoneTree.Comparers;
using ZoneTree.Serializers;
using Xunit;
using Xunit.Abstractions;

namespace Bielu.PersistentQueues.Tests.Storage.ZoneTree;

/// <summary>
/// Runs the shared MessageStoreTests against the ZoneTree provider.
/// </summary>
public class ZoneTreeMessageStoreTests(ITestOutputHelper output) : MessageStoreTests(output)
{
    protected override IMessageStore CreateStoreForPath(string path)
    {
        return new ZoneTreeMessageStore(path, new MessageSerializer());
    }

    [Fact]
    public void open_transaction_does_not_block_unrelated_writes()
    {
        var store = CreateStore();
        var disposeStore = true;
        IStoreTransaction? tx = null;
        using var writeCompleted = new ManualResetEventSlim();
        Exception? writeException = null;

        try
        {
            store.CreateQueue("test");
            tx = store.BeginTransaction();
            store.StoreIncoming(tx, NewMessage("test", "pending"));

            var writer = new Thread(() =>
            {
                try
                {
                    store.StoreIncoming(NewMessage("test", "concurrent"));
                }
                catch (Exception ex)
                {
                    writeException = ex;
                }
                finally
                {
                    writeCompleted.Set();
                }
            })
            {
                IsBackground = true
            };

            writer.Start();

            if (!writeCompleted.Wait(TimeSpan.FromSeconds(2)))
            {
                tx.Dispose();
                tx = null;
                disposeStore = writeCompleted.Wait(TimeSpan.FromSeconds(2));
                writeCompleted.IsSet.ShouldBeTrue("The unrelated write stayed blocked while a transaction was open.");
            }

            writeException.ShouldBeNull();

            tx!.Commit();
            store.GetMessageCount("test").ShouldBe(2);
        }
        finally
        {
            tx?.Dispose();
            if (disposeStore)
            {
                store.Dispose();
            }
        }
    }

    [Fact]
    public void concurrent_writes_share_the_single_zonetree_keyspace()
    {
        StorageScenario(store =>
        {
            var tasks = Enumerable.Range(0, 8)
                .Select(worker => Task.Run(() =>
                {
                    for (var i = 0; i < 50; i++)
                    {
                        store.StoreIncoming(NewMessage("test", $"{worker}-{i}"));
                    }
                }))
                .ToArray();

            Task.WaitAll(tasks);

            store.GetMessageCount("test").ShouldBe(400);
        });
    }

    [Fact]
    public void migrates_legacy_per_queue_layout()
    {
        var path = TempPath();
        var serializer = new MessageSerializer();
        var message = NewMessage("legacy", "from old layout");
        var queuePath = Path.Combine(path, "legacy");
        Directory.CreateDirectory(queuePath);
        File.WriteAllText(Path.Combine(queuePath, ".queue-name"), "legacy");

        using (var legacyTree = new ZoneTreeFactory<Guid, Memory<byte>>()
            .SetDataDirectory(queuePath)
            .SetComparer(new GuidComparerAscending())
            .SetKeySerializer(new StructSerializer<Guid>())
            .SetValueSerializer(new ByteArraySerializer())
            .SetIsDeletedDelegate((in Guid _, in Memory<byte> value) => value.Length == 0)
            .SetMarkValueDeletedDelegate((ref Memory<byte> value) => value = Memory<byte>.Empty)
            .OpenOrCreate())
        {
            legacyTree.Upsert(message.Id.MessageIdentifier, serializer.AsSpan(message).ToArray());
            legacyTree.Maintenance.SaveMetaData();
        }

        using var store = CreateStoreForPath(path);

        store.GetAllQueues().ShouldContain("legacy");
        store.PersistedIncoming("legacy").Single().Id.ShouldBe(message.Id);
    }

    [Fact]
    public void migrates_legacy_per_queue_layout_without_metadata_file()
    {
        var path = TempPath();
        var serializer = new MessageSerializer();
        var queueName = "legacy queue";
        var message = NewMessage(queueName, "from metadata-less old layout");
        var queuePath = Path.Combine(path, "legacy%20queue");
        Directory.CreateDirectory(queuePath);

        using (var legacyTree = new ZoneTreeFactory<Guid, Memory<byte>>()
            .SetDataDirectory(queuePath)
            .SetComparer(new GuidComparerAscending())
            .SetKeySerializer(new StructSerializer<Guid>())
            .SetValueSerializer(new ByteArraySerializer())
            .SetIsDeletedDelegate((in Guid _, in Memory<byte> value) => value.Length == 0)
            .SetMarkValueDeletedDelegate((ref Memory<byte> value) => value = Memory<byte>.Empty)
            .OpenOrCreate())
        {
            legacyTree.Upsert(message.Id.MessageIdentifier, serializer.AsSpan(message).ToArray());
            legacyTree.Maintenance.SaveMetaData();
        }

        using var store = CreateStoreForPath(path);

        store.GetAllQueues().ShouldContain(queueName);
        store.PersistedIncoming(queueName).Single().Id.ShouldBe(message.Id);
    }

    [Fact]
    public void retries_legacy_migration_when_a_legacy_queue_fails_to_migrate()
    {
        var path = TempPath();
        var serializer = new MessageSerializer();
        var message = NewMessage("good", "from good legacy queue");
        var goodQueuePath = Path.Combine(path, "good");
        Directory.CreateDirectory(goodQueuePath);
        File.WriteAllText(Path.Combine(goodQueuePath, ".queue-name"), "good");

        using (var legacyTree = new ZoneTreeFactory<Guid, Memory<byte>>()
            .SetDataDirectory(goodQueuePath)
            .SetComparer(new GuidComparerAscending())
            .SetKeySerializer(new StructSerializer<Guid>())
            .SetValueSerializer(new ByteArraySerializer())
            .SetIsDeletedDelegate((in Guid _, in Memory<byte> value) => value.Length == 0)
            .SetMarkValueDeletedDelegate((ref Memory<byte> value) => value = Memory<byte>.Empty)
            .OpenOrCreate())
        {
            legacyTree.Upsert(message.Id.MessageIdentifier, serializer.AsSpan(message).ToArray());
            legacyTree.Maintenance.SaveMetaData();
        }

        var failingQueuePath = Path.Combine(path, "failing");
        Directory.CreateDirectory(failingQueuePath);
        File.WriteAllText(Path.Combine(failingQueuePath, ".queue-name"), "failing");
        File.WriteAllText(Path.Combine(failingQueuePath, "0.json"), "not zonetree metadata");

        Should.Throw<InvalidOperationException>(() =>
        {
            using var _ = CreateStoreForPath(path);
        });

        Directory.Delete(failingQueuePath, recursive: true);

        using var store = CreateStoreForPath(path);

        store.GetAllQueues().ShouldContain("good");
        store.PersistedIncoming("good").Single().Id.ShouldBe(message.Id);
    }
}
