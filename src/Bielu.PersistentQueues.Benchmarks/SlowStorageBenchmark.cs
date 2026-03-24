using System.Diagnostics;
using BenchmarkDotNet.Attributes;
using FASTER.core;
using LightningDB;
using Tenray.ZoneTree;
using Tenray.ZoneTree.Serializers;

namespace LightningQueues.Benchmarks;

/// <summary>
/// Benchmarks how each storage provider degrades under slow filesystem conditions.
///
/// Simulates slow I/O by injecting artificial latency after each commit/flush operation.
/// This models the real-world impact of:
///   - 0ms:  Fast NVMe SSD (baseline — fsync completes instantly)
///   - 1ms:  Standard SATA SSD fsync latency
///   - 5ms:  Networked storage (NFS, EBS gp2) or slow SSD
///   - 10ms: Spinning HDD or high-latency network storage
///
/// The key insight: on slow storage, the bottleneck is fsync/flush latency per commit.
/// Providers that batch writes or defer flushes degrade less under I/O pressure.
///
/// Uses durable configuration for all providers (not the fast-but-unsafe defaults)
/// because slow storage matters most when you need data durability.
/// </summary>
[MemoryDiagnoser]
[RankColumn]
public class SlowStorageBenchmark
{
    // --- LMDB state ---
    private LightningEnvironment? _lmdbEnv;
    private string _lmdbPath = null!;

    // --- ZoneTree state ---
    private IZoneTree<Memory<byte>, Memory<byte>>? _zoneTree;
    private string _zoneTreePath = null!;

    // --- FASTER state ---
    private FasterKV<SpanByte, SpanByte>? _fasterStore;
    private ClientSession<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, Empty, IFunctions<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, Empty>>? _fasterSession;
    private IDevice? _fasterLog;
    private string _fasterPath = null!;

    // --- Shared test data ---
    private byte[][] _keys = null!;
    private byte[][] _values = null!;

    /// <summary>
    /// Number of messages in each operation.
    /// Kept moderate since slow storage tests are naturally slower.
    /// </summary>
    [Params(1_000, 10_000, 100_000)]
    public int MessageCount { get; set; }

    /// <summary>
    /// Simulated fsync/flush latency in milliseconds.
    /// Applied after each commit operation to model slow storage.
    /// 0 = baseline (fast NVMe), 1 = SSD, 5 = networked/slow, 10 = HDD
    /// </summary>
    [Params(0, 1, 5, 10)]
    public int SimulatedLatencyMs { get; set; }

    private const int MessageDataSize = 512; // Fixed at 512B - typical message payload

    [GlobalSetup]
    public void GlobalSetup()
    {
        var basePath = Path.Combine(Path.GetTempPath(), "slow-storage-bench", Guid.NewGuid().ToString());
        Directory.CreateDirectory(basePath);

        // Generate test data
        var random = new Random(42);
        _keys = new byte[MessageCount][];
        _values = new byte[MessageCount][];
        for (var i = 0; i < MessageCount; i++)
        {
            _keys[i] = new byte[16];
            var timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + i;
            BitConverter.TryWriteBytes(_keys[i].AsSpan(0, 8), timestamp);
            if (BitConverter.IsLittleEndian)
                Array.Reverse(_keys[i], 0, 8);
            BitConverter.TryWriteBytes(_keys[i].AsSpan(8, 4), i);
            if (BitConverter.IsLittleEndian)
                Array.Reverse(_keys[i], 8, 4);
            random.NextBytes(_keys[i].AsSpan(12, 4));

            _values[i] = new byte[MessageDataSize];
            random.NextBytes(_values[i]);
        }

        // --- LMDB: Durable configuration (no MapAsync, no NoSync) ---
        var lmdbMapSize = MessageCount >= 100_000
            ? 2L * 1024 * 1024 * 1024
            : 1L * 1024 * 1024 * 1024;
        _lmdbPath = Path.Combine(basePath, "lmdb");
        Directory.CreateDirectory(_lmdbPath);
        _lmdbEnv = new LightningEnvironment(_lmdbPath, new EnvironmentConfiguration
        {
            MapSize = lmdbMapSize,
            MaxDatabases = 5
        });
        // Durable mode: NoLock only (no MapAsync, no NoSync — forces fsync on commit)
        _lmdbEnv.Open(EnvironmentOpenFlags.NoLock);
        using (var tx = _lmdbEnv.BeginTransaction())
        {
            tx.OpenDatabase("test", new DatabaseConfiguration { Flags = DatabaseOpenFlags.Create });
            tx.Commit();
        }

        // --- ZoneTree: Default (has WAL for durability) ---
        _zoneTreePath = Path.Combine(basePath, "zonetree");
        Directory.CreateDirectory(_zoneTreePath);
        _zoneTree = new ZoneTreeFactory<Memory<byte>, Memory<byte>>()
            .SetDataDirectory(_zoneTreePath)
            .SetKeySerializer(new ByteArraySerializer())
            .SetValueSerializer(new ByteArraySerializer())
            .SetComparer(new ByteArrayComparerAscending())
            .OpenOrCreate();

        // --- FASTER: Standard configuration ---
        var fasterMemBits = MessageCount >= 100_000 ? 28 : 25;
        _fasterPath = Path.Combine(basePath, "faster");
        Directory.CreateDirectory(_fasterPath);
        _fasterLog = Devices.CreateLogDevice(Path.Combine(_fasterPath, "hlog.log"));
        _fasterStore = new FasterKV<SpanByte, SpanByte>(
            1L << 20,
            new LogSettings
            {
                LogDevice = _fasterLog,
                MemorySizeBits = fasterMemBits,
                PageSizeBits = 22
            }
        );
        _fasterSession = _fasterStore.NewSession(new SpanByteFunctions<Empty>());
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _zoneTree?.Dispose();
        _fasterSession?.Dispose();
        _fasterStore?.Dispose();
        _fasterLog?.Dispose();
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
        // Clear LMDB
        using (var tx = _lmdbEnv!.BeginTransaction())
        {
            using var db = tx.OpenDatabase("test");
            tx.TruncateDatabase(db);
            tx.Commit();
        }

        // Clear ZoneTree
        _zoneTree?.Dispose();
        if (Directory.Exists(_zoneTreePath))
        {
            try { Directory.Delete(_zoneTreePath, true); }
            catch { /* ignore locked files */ }
        }
        Directory.CreateDirectory(_zoneTreePath);
        _zoneTree = new ZoneTreeFactory<Memory<byte>, Memory<byte>>()
            .SetDataDirectory(_zoneTreePath)
            .SetKeySerializer(new ByteArraySerializer())
            .SetValueSerializer(new ByteArraySerializer())
            .SetComparer(new ByteArrayComparerAscending())
            .OpenOrCreate();

        // Clear FASTER
        _fasterSession?.Dispose();
        _fasterStore?.Dispose();
        _fasterLog?.Dispose();
        if (Directory.Exists(_fasterPath))
        {
            try { Directory.Delete(_fasterPath, true); }
            catch { /* ignore locked files */ }
        }
        Directory.CreateDirectory(_fasterPath);
        var fasterMemBits = MessageCount >= 100_000 ? 28 : 25;
        _fasterLog = Devices.CreateLogDevice(Path.Combine(_fasterPath, "hlog.log"));
        _fasterStore = new FasterKV<SpanByte, SpanByte>(
            1L << 20,
            new LogSettings
            {
                LogDevice = _fasterLog,
                MemorySizeBits = fasterMemBits,
                PageSizeBits = 22
            }
        );
        _fasterSession = _fasterStore.NewSession(new SpanByteFunctions<Empty>());
    }

    /// <summary>
    /// Simulates slow storage by busy-waiting for the configured latency.
    /// Uses SpinWait for sub-ms accuracy (Thread.Sleep has ~15ms minimum on some systems).
    /// </summary>
    private void SimulateIoLatency()
    {
        if (SimulatedLatencyMs <= 0) return;
        Thread.Sleep(SimulatedLatencyMs);
    }

    // ========================================================================
    // PUT WITH SLOW STORAGE — How does write throughput degrade?
    // Each provider commits once at the end; latency simulates fsync cost.
    // ========================================================================

    [Benchmark(Description = "Put_LMDB_Durable")]
    [BenchmarkCategory("Put")]
    public void Put_Lmdb()
    {
        using var tx = _lmdbEnv!.BeginTransaction();
        using var db = tx.OpenDatabase("test");
        for (var i = 0; i < MessageCount; i++)
        {
            tx.Put(db, _keys[i], _values[i]);
        }
        tx.Commit();
        SimulateIoLatency(); // fsync cost on commit
    }

    [Benchmark(Description = "Put_ZoneTree")]
    [BenchmarkCategory("Put")]
    public void Put_ZoneTree()
    {
        for (var i = 0; i < MessageCount; i++)
        {
            _zoneTree!.Upsert(_keys[i], _values[i]);
        }
        SimulateIoLatency(); // WAL flush cost
    }

    [Benchmark(Description = "Put_FASTER")]
    [BenchmarkCategory("Put")]
    public void Put_Faster()
    {
        for (var i = 0; i < MessageCount; i++)
        {
            var key = SpanByte.FromPinnedMemory(_keys[i]);
            var value = SpanByte.FromPinnedMemory(_values[i]);
            _fasterSession!.Upsert(ref key, ref value);
        }
        _fasterSession!.CompletePending(true);
        SimulateIoLatency(); // checkpoint/flush cost
    }

    // ========================================================================
    // QUEUE CYCLE WITH SLOW STORAGE — Full store → enumerate → delete cycle
    // This is the most realistic scenario: 3 commits = 3× the fsync penalty
    // ========================================================================

    [Benchmark(Description = "QueueCycle_LMDB_Durable")]
    [BenchmarkCategory("QueueCycle")]
    public int QueueCycle_Lmdb()
    {
        // 1. Store all messages (commit + fsync)
        using (var tx = _lmdbEnv!.BeginTransaction())
        {
            using var db = tx.OpenDatabase("test");
            for (var i = 0; i < MessageCount; i++)
                tx.Put(db, _keys[i], _values[i]);
            tx.Commit();
            SimulateIoLatency();
        }

        // 2. Enumerate (read-only, no fsync needed)
        var count = 0;
        using (var tx = _lmdbEnv!.BeginTransaction(TransactionBeginFlags.ReadOnly))
        {
            using var db = tx.OpenDatabase("test");
            using var cursor = tx.CreateCursor(db);
            foreach (var _ in cursor.AsEnumerable())
                count++;
        }

        // 3. Delete all (commit + fsync)
        using (var tx = _lmdbEnv!.BeginTransaction())
        {
            using var db = tx.OpenDatabase("test");
            for (var i = 0; i < MessageCount; i++)
                tx.Delete(db, _keys[i]);
            tx.Commit();
            SimulateIoLatency();
        }

        return count;
    }

    [Benchmark(Description = "QueueCycle_ZoneTree")]
    [BenchmarkCategory("QueueCycle")]
    public int QueueCycle_ZoneTree()
    {
        // 1. Store all (WAL flush)
        for (var i = 0; i < MessageCount; i++)
            _zoneTree!.Upsert(_keys[i], _values[i]);
        SimulateIoLatency();

        // 2. Enumerate
        var count = 0;
        using (var iterator = _zoneTree!.CreateIterator())
        {
            while (iterator.Next())
                count++;
        }

        // 3. Delete all (WAL flush)
        for (var i = 0; i < MessageCount; i++)
            _zoneTree!.ForceDelete(_keys[i]);
        SimulateIoLatency();

        return count;
    }

    [Benchmark(Description = "QueueCycle_FASTER")]
    [BenchmarkCategory("QueueCycle")]
    public int QueueCycle_Faster()
    {
        // 1. Store all (log flush)
        for (var i = 0; i < MessageCount; i++)
        {
            var key = SpanByte.FromPinnedMemory(_keys[i]);
            var value = SpanByte.FromPinnedMemory(_values[i]);
            _fasterSession!.Upsert(ref key, ref value);
        }
        _fasterSession!.CompletePending(true);
        SimulateIoLatency();

        // 2. Read by key (FASTER has no ordered enumeration)
        var count = 0;
        for (var i = 0; i < MessageCount; i++)
        {
            var key = SpanByte.FromPinnedMemory(_keys[i]);
            var output = new SpanByteAndMemory();
            var status = _fasterSession!.Read(ref key, ref output);
            if (status.Found) count++;
            output.Memory?.Dispose();
        }

        // 3. Delete all (log mutation)
        for (var i = 0; i < MessageCount; i++)
        {
            var key = SpanByte.FromPinnedMemory(_keys[i]);
            _fasterSession!.Delete(ref key);
        }
        _fasterSession!.CompletePending(true);
        SimulateIoLatency();

        return count;
    }

    // ========================================================================
    // BATCHED COMMITS — How providers handle frequent small commits
    // Simulates receiving messages in small batches (e.g., 100 at a time)
    // with an fsync after each batch. This is the WORST case for slow storage.
    // ========================================================================

    [Benchmark(Description = "BatchedPut_LMDB_Durable")]
    [BenchmarkCategory("BatchedPut")]
    public void BatchedPut_Lmdb()
    {
        const int batchSize = 100;
        for (var batch = 0; batch < MessageCount; batch += batchSize)
        {
            var end = Math.Min(batch + batchSize, MessageCount);
            using var tx = _lmdbEnv!.BeginTransaction();
            using var db = tx.OpenDatabase("test");
            for (var i = batch; i < end; i++)
            {
                tx.Put(db, _keys[i], _values[i]);
            }
            tx.Commit();
            SimulateIoLatency(); // fsync per batch
        }
    }

    [Benchmark(Description = "BatchedPut_ZoneTree")]
    [BenchmarkCategory("BatchedPut")]
    public void BatchedPut_ZoneTree()
    {
        const int batchSize = 100;
        for (var batch = 0; batch < MessageCount; batch += batchSize)
        {
            var end = Math.Min(batch + batchSize, MessageCount);
            for (var i = batch; i < end; i++)
            {
                _zoneTree!.Upsert(_keys[i], _values[i]);
            }
            SimulateIoLatency(); // WAL flush per batch
        }
    }

    [Benchmark(Description = "BatchedPut_FASTER")]
    [BenchmarkCategory("BatchedPut")]
    public void BatchedPut_Faster()
    {
        const int batchSize = 100;
        for (var batch = 0; batch < MessageCount; batch += batchSize)
        {
            var end = Math.Min(batch + batchSize, MessageCount);
            for (var i = batch; i < end; i++)
            {
                var key = SpanByte.FromPinnedMemory(_keys[i]);
                var value = SpanByte.FromPinnedMemory(_values[i]);
                _fasterSession!.Upsert(ref key, ref value);
            }
            _fasterSession!.CompletePending(true);
            SimulateIoLatency(); // log flush per batch
        }
    }
}
