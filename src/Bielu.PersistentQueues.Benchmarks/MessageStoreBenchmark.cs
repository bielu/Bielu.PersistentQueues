using BenchmarkDotNet.Attributes;
using LightningDB;
using Bielu.PersistentQueues.Serialization;
using Bielu.PersistentQueues.Storage;
using Bielu.PersistentQueues.Storage.LMDB;
using Bielu.PersistentQueues.Storage.ZoneTree;

namespace Bielu.PersistentQueues.Benchmarks;

/// <summary>
/// Compares LMDB and ZoneTree at the IMessageStore abstraction level.
/// This benchmarks what users actually experience: serialized message round-trips
/// through the full storage stack including serialization, transactions, and I/O.
///
/// Operations benchmarked:
///   1. StoreIncoming: Batch insert messages into a queue
///   2. GetMessage: Point lookup by MessageId
///   3. PersistedIncoming: Enumerate all messages in a queue
///   4. DeleteIncoming: Remove messages from a queue
///   5. QueueCycle: Full store → enumerate → delete cycle (most realistic)
///   6. StoreOutgoing + SuccessfullySent: Outgoing message lifecycle
/// </summary>
[MemoryDiagnoser]
[RankColumn]
public class MessageStoreBenchmark
{
    private IMessageStore? _lmdbStore;
    private IMessageStore? _zoneTreeStore;
    private LightningEnvironment? _lmdbEnv;
    private string _lmdbPath = null!;
    private string _zoneTreePath = null!;

    private Message[]? _messages;

    [Params(100, 1_000)]
    public int MessageCount { get; set; }

    [Params(64, 512)]
    public int MessageDataSize { get; set; }

    [GlobalSetup]
    public void GlobalSetup()
    {
        var basePath = Path.Combine(Path.GetTempPath(), "msgstore-bench", Guid.NewGuid().ToString());
        var serializer = new MessageSerializer();

        // --- LMDB Store ---
        _lmdbPath = Path.Combine(basePath, "lmdb");
        Directory.CreateDirectory(_lmdbPath);
        _lmdbEnv = new LightningEnvironment(_lmdbPath, new EnvironmentConfiguration
        {
            MapSize = 1024 * 1024 * 500, // 500MB
            MaxDatabases = 5
        });
        _lmdbStore = new LmdbMessageStore(_lmdbEnv, serializer);
        _lmdbStore.CreateQueue("test");

        // --- ZoneTree Store ---
        _zoneTreePath = Path.Combine(basePath, "zonetree");
        Directory.CreateDirectory(_zoneTreePath);
        _zoneTreeStore = new ZoneTreeMessageStore(_zoneTreePath, serializer);
        _zoneTreeStore.CreateQueue("test");

        // --- Generate test messages ---
        _messages = new Message[MessageCount];
        var random = new Random(42);
        for (var i = 0; i < MessageCount; i++)
        {
            var data = new byte[MessageDataSize];
            random.NextBytes(data);
            _messages[i] = Message.Create(data: data, queue: "test");
        }
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _lmdbStore?.Dispose();
        _zoneTreeStore?.Dispose();
        _lmdbEnv?.Dispose();

        try
        {
            var basePath = Path.GetDirectoryName(_lmdbPath);
            if (basePath != null && Directory.Exists(basePath))
                Directory.Delete(basePath, true);
        }
        catch { /* ignore cleanup errors */ }
    }

    [IterationSetup]
    public void IterationSetup()
    {
        _lmdbStore!.ClearAllStorage();
        _zoneTreeStore!.ClearAllStorage();

        // Regenerate messages with fresh IDs to avoid duplicate key issues
        var random = new Random(42);
        for (var i = 0; i < MessageCount; i++)
        {
            var data = new byte[MessageDataSize];
            random.NextBytes(data);
            _messages![i] = Message.Create(data: data, queue: "test");
        }
    }

    // ========================================================================
    // STORE INCOMING — Batch insert messages
    // ========================================================================

    [Benchmark(Description = "StoreIncoming_LMDB")]
    [BenchmarkCategory("StoreIncoming")]
    public void StoreIncoming_Lmdb()
    {
        _lmdbStore!.StoreIncoming(_messages!);
    }

    [Benchmark(Description = "StoreIncoming_ZoneTree")]
    [BenchmarkCategory("StoreIncoming")]
    public void StoreIncoming_ZoneTree()
    {
        _zoneTreeStore!.StoreIncoming(_messages!);
    }

    // ========================================================================
    // GET MESSAGE — Point lookup by MessageId
    // ========================================================================

    [Benchmark(Description = "GetMessage_LMDB")]
    [BenchmarkCategory("GetMessage")]
    public int GetMessage_Lmdb()
    {
        _lmdbStore!.StoreIncoming(_messages!);
        var count = 0;
        foreach (var msg in _messages!)
        {
            if (_lmdbStore.GetMessage("test", msg.Id) != null) count++;
        }
        return count;
    }

    [Benchmark(Description = "GetMessage_ZoneTree")]
    [BenchmarkCategory("GetMessage")]
    public int GetMessage_ZoneTree()
    {
        _zoneTreeStore!.StoreIncoming(_messages!);
        var count = 0;
        foreach (var msg in _messages!)
        {
            if (_zoneTreeStore.GetMessage("test", msg.Id) != null) count++;
        }
        return count;
    }

    // ========================================================================
    // PERSISTED INCOMING — Enumerate all messages in a queue
    // ========================================================================

    [Benchmark(Description = "PersistedIncoming_LMDB")]
    [BenchmarkCategory("PersistedIncoming")]
    public int PersistedIncoming_Lmdb()
    {
        _lmdbStore!.StoreIncoming(_messages!);
        var count = 0;
        foreach (var _ in _lmdbStore.PersistedIncoming("test"))
        {
            count++;
        }
        return count;
    }

    [Benchmark(Description = "PersistedIncoming_ZoneTree")]
    [BenchmarkCategory("PersistedIncoming")]
    public int PersistedIncoming_ZoneTree()
    {
        _zoneTreeStore!.StoreIncoming(_messages!);
        var count = 0;
        foreach (var _ in _zoneTreeStore.PersistedIncoming("test"))
        {
            count++;
        }
        return count;
    }

    // ========================================================================
    // DELETE INCOMING — Remove messages
    // ========================================================================

    [Benchmark(Description = "DeleteIncoming_LMDB")]
    [BenchmarkCategory("DeleteIncoming")]
    public void DeleteIncoming_Lmdb()
    {
        _lmdbStore!.StoreIncoming(_messages!);
        _lmdbStore.DeleteIncoming(_messages!);
    }

    [Benchmark(Description = "DeleteIncoming_ZoneTree")]
    [BenchmarkCategory("DeleteIncoming")]
    public void DeleteIncoming_ZoneTree()
    {
        _zoneTreeStore!.StoreIncoming(_messages!);
        _zoneTreeStore.DeleteIncoming(_messages!);
    }

    // ========================================================================
    // QUEUE CYCLE — Full store → enumerate → delete (most realistic)
    // ========================================================================

    [Benchmark(Description = "QueueCycle_LMDB")]
    [BenchmarkCategory("QueueCycle")]
    public int QueueCycle_Lmdb()
    {
        // 1. Store
        _lmdbStore!.StoreIncoming(_messages!);

        // 2. Enumerate
        var count = 0;
        foreach (var _ in _lmdbStore.PersistedIncoming("test"))
            count++;

        // 3. Delete
        _lmdbStore.DeleteIncoming(_messages!);

        return count;
    }

    [Benchmark(Description = "QueueCycle_ZoneTree")]
    [BenchmarkCategory("QueueCycle")]
    public int QueueCycle_ZoneTree()
    {
        // 1. Store
        _zoneTreeStore!.StoreIncoming(_messages!);

        // 2. Enumerate
        var count = 0;
        foreach (var _ in _zoneTreeStore.PersistedIncoming("test"))
            count++;

        // 3. Delete
        _zoneTreeStore.DeleteIncoming(_messages!);

        return count;
    }

    // ========================================================================
    // OUTGOING LIFECYCLE — Store → SuccessfullySent
    // ========================================================================

    [Benchmark(Description = "OutgoingCycle_LMDB")]
    [BenchmarkCategory("OutgoingCycle")]
    public void OutgoingCycle_Lmdb()
    {
        // Store outgoing
        _lmdbStore!.StoreOutgoing(_messages!.Select(m =>
            Message.Create(data: m.DataArray, queue: "test", destinationUri: "lq.tcp://localhost:5050")));

        // Mark as sent
        foreach (var msg in _lmdbStore.PersistedOutgoing().ToList())
        {
            _lmdbStore.SuccessfullySent(msg);
        }
    }

    [Benchmark(Description = "OutgoingCycle_ZoneTree")]
    [BenchmarkCategory("OutgoingCycle")]
    public void OutgoingCycle_ZoneTree()
    {
        // Store outgoing
        _zoneTreeStore!.StoreOutgoing(_messages!.Select(m =>
            Message.Create(data: m.DataArray, queue: "test", destinationUri: "lq.tcp://localhost:5050")));

        // Mark as sent
        foreach (var msg in _zoneTreeStore.PersistedOutgoing().ToList())
        {
            _zoneTreeStore.SuccessfullySent(msg);
        }
    }
}
