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

    /// <summary>
    /// Prepares the benchmark environment by creating a unique temporary base directory, initializing LMDB and ZoneTree message stores (each with a "test" queue), and generating deterministic test messages.
    /// </summary>
    /// <remarks>
    /// Initializes the fields _lmdbEnv, _lmdbStore, _zoneTreeStore, _lmdbPath, _zoneTreePath and populates _messages with MessageCount messages of MessageDataSize bytes using a fixed random seed for deterministic content.
    /// </remarks>
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

    /// <summary>
    /// Releases allocated message-store resources and attempts to delete the temporary benchmark directory.
    /// </summary>
    /// <remarks>
    /// Disposes the LMDB and ZoneTree stores and the LMDB environment. It then attempts to delete the parent directory of the LMDB path; any errors during deletion are ignored.
    /// </remarks>
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

    /// <summary>
    /// Prepare a fresh benchmark iteration by clearing both message stores and regenerating the message array with new message IDs.
    /// </summary>
    /// <remarks>
    /// Clears all persisted data from the LMDB and ZoneTree stores, then repopulates the <c>_messages</c> array using deterministic random content (seed 42) so each message receives a new <c>Id</c>, avoiding duplicate-key collisions during the iteration.
    /// </remarks>
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
    /// <summary>
    /// Persists the benchmark's generated messages into the LMDB-backed message store.
    /// </summary>

    [Benchmark(Description = "StoreIncoming_LMDB")]
    [BenchmarkCategory("StoreIncoming")]
    public void StoreIncoming_Lmdb()
    {
        _lmdbStore!.StoreIncoming(_messages!);
    }

    /// <summary>
    /// Stores the current set of benchmark messages into the ZoneTree-backed message store.
    /// </summary>
    [Benchmark(Description = "StoreIncoming_ZoneTree")]
    [BenchmarkCategory("StoreIncoming")]
    public void StoreIncoming_ZoneTree()
    {
        _zoneTreeStore!.StoreIncoming(_messages!);
    }

    // ========================================================================
    // GET MESSAGE — Point lookup by MessageId
    /// <summary>
    /// Looks up each generated message by ID in the LMDB-backed message store and counts how many are found.
    /// </summary>
    /// <returns>The number of messages that were successfully retrieved from the LMDB store.</returns>

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

    /// <summary>
    /// Stores the benchmark messages into the ZoneTree store, looks up each by ID in the "test" queue, and counts how many are present.
    /// </summary>
    /// <returns>The number of messages successfully retrieved from the ZoneTree store.</returns>
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
    /// <summary>
    /// Stores the benchmark messages into the LMDB-backed store, enumerates all persisted incoming messages in the "test" queue, and returns how many were found.
    /// </summary>
    /// <returns>The number of persisted incoming messages in the "test" queue.</returns>

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

    /// <summary>
    /// Stores the benchmark messages into the ZoneTree message store and counts persisted incoming messages in the "test" queue.
    /// </summary>
    /// <returns>The number of persisted incoming messages found in the "test" queue.</returns>
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
    /// <summary>
    /// Stores the benchmark messages into the LMDB-backed message store and then deletes those messages.
    /// </summary>

    [Benchmark(Description = "DeleteIncoming_LMDB")]
    [BenchmarkCategory("DeleteIncoming")]
    public void DeleteIncoming_Lmdb()
    {
        _lmdbStore!.StoreIncoming(_messages!);
        _lmdbStore.DeleteIncoming(_messages!);
    }

    /// <summary>
    /// Stores the configured benchmark messages into the ZoneTree message store and then deletes them.
    /// </summary>
    [Benchmark(Description = "DeleteIncoming_ZoneTree")]
    [BenchmarkCategory("DeleteIncoming")]
    public void DeleteIncoming_ZoneTree()
    {
        _zoneTreeStore!.StoreIncoming(_messages!);
        _zoneTreeStore.DeleteIncoming(_messages!);
    }

    // ========================================================================
    // QUEUE CYCLE — Full store → enumerate → delete (most realistic)
    /// <summary>
    /// Runs a full queue lifecycle on the LMDB-backed store for the "test" queue: store incoming messages, enumerate persisted incoming messages, then delete them.
    /// </summary>
    /// <returns>The number of persisted incoming messages observed during enumeration.</returns>

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

    /// <summary>
    /// Performs a full queue cycle against the ZoneTree store: stores the benchmark messages, enumerates all persisted incoming messages for the "test" queue, then deletes the stored messages.
    /// </summary>
    /// <returns>The number of messages enumerated from the "test" queue.</returns>
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
    /// <summary>
    /// Stores outgoing messages for the benchmark queue and then marks all persisted outgoing messages as successfully sent.
    /// </summary>

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

    /// <summary>
    /// Stores outgoing messages for the "test" queue with a fixed destination and then marks all persisted outgoing messages as successfully sent.
    /// </summary>
    /// <remarks>
    /// Creates outgoing messages from the benchmark's message set, persists them, enumerates the persisted outgoing messages, and acknowledges each as successfully sent.
    /// </remarks>
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
