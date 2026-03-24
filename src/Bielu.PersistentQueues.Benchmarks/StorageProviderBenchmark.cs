using BenchmarkDotNet.Attributes;
using FASTER.core;
using LightningDB;
using LightningQueues.Serialization;
using LightningQueues.Storage.LMDB;
using Tenray.ZoneTree;
using Tenray.ZoneTree.Comparers;
using Tenray.ZoneTree.Serializers;

namespace LightningQueues.Benchmarks;

/// <summary>
/// Compares three storage providers for LightningQueues message queue patterns:
///   - LMDB (current) via LightningDB
///   - ZoneTree (pure C#, LSM tree)
///   - FASTER KV (Microsoft, hash-based)
///
/// Operations benchmarked match real queue usage:
///   1. Put: Store messages (equivalent to StoreIncoming)
///   2. Get: Retrieve message by key (equivalent to GetMessage)
///   3. Enumerate: Iterate all messages in order (equivalent to PersistedIncoming)
///   4. Delete: Remove messages (equivalent to DeleteIncoming/SuccessfullySent)
///   5. Mixed: Store-then-delete cycle (simulates send-and-acknowledge)
/// </summary>
[MemoryDiagnoser]
[RankColumn]
public class StorageProviderBenchmark
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

    [Params(100, 1_000, 10_000, 100_000, 1_000_000)]
    public int MessageCount { get; set; }

    [Params(64, 512)]
    public int MessageDataSize { get; set; }

    [GlobalSetup]
    public void GlobalSetup()
    {
        var basePath = Path.Combine(Path.GetTempPath(), "storage-bench", Guid.NewGuid().ToString());
        Directory.CreateDirectory(basePath);

        // Generate test data - 16-byte keys (like MessageId GUIDs) and variable-size values
        var random = new Random(42);
        _keys = new byte[MessageCount][];
        _values = new byte[MessageCount][];
        for (var i = 0; i < MessageCount; i++)
        {
            // Generate sequential COMB-like keys (ascending order) to match real usage
            _keys[i] = new byte[16];
            // Timestamp-based prefix for ordering (big-endian timestamp + counter)
            var timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + i;
            BitConverter.TryWriteBytes(_keys[i].AsSpan(0, 8), timestamp);
            if (BitConverter.IsLittleEndian)
                Array.Reverse(_keys[i], 0, 8); // big-endian for sort order
            BitConverter.TryWriteBytes(_keys[i].AsSpan(8, 4), i);
            if (BitConverter.IsLittleEndian)
                Array.Reverse(_keys[i], 8, 4);
            random.NextBytes(_keys[i].AsSpan(12, 4));

            _values[i] = new byte[MessageDataSize];
            random.NextBytes(_values[i]);
        }

        // --- Initialize LMDB ---
        // Scale map size based on message count to avoid running out of space
        var lmdbMapSize = MessageCount switch
        {
            >= 1_000_000 => 4L * 1024 * 1024 * 1024,  // 4GB for 1M messages
            >= 100_000 => 2L * 1024 * 1024 * 1024,     // 2GB for 100K messages
            _ => 1L * 1024 * 1024 * 1024                // 1GB default
        };
        _lmdbPath = Path.Combine(basePath, "lmdb");
        Directory.CreateDirectory(_lmdbPath);
        _lmdbEnv = new LightningEnvironment(_lmdbPath, new EnvironmentConfiguration
        {
            MapSize = lmdbMapSize,
            MaxDatabases = 5
        });
        _lmdbEnv.Open(EnvironmentOpenFlags.NoLock | EnvironmentOpenFlags.MapAsync);
        using (var tx = _lmdbEnv.BeginTransaction())
        {
            tx.OpenDatabase("test", new DatabaseConfiguration { Flags = DatabaseOpenFlags.Create });
            tx.Commit();
        }

        // --- Initialize ZoneTree ---
        _zoneTreePath = Path.Combine(basePath, "zonetree");
        Directory.CreateDirectory(_zoneTreePath);
        _zoneTree = new ZoneTreeFactory<Memory<byte>, Memory<byte>>()
            .SetDataDirectory(_zoneTreePath)
            .SetKeySerializer(new ByteArraySerializer())
            .SetValueSerializer(new ByteArraySerializer())
            .SetComparer(new ByteArrayComparerAscending())
            .OpenOrCreate();

        // --- Initialize FASTER ---
        // Scale FASTER in-memory log size based on message count
        var fasterMemoryBits = MessageCount switch
        {
            >= 1_000_000 => 30, // 1GB in-memory log
            >= 100_000 => 28,   // 256MB in-memory log
            _ => 25             // 32MB in-memory log
        };
        _fasterPath = Path.Combine(basePath, "faster");
        Directory.CreateDirectory(_fasterPath);
        _fasterLog = Devices.CreateLogDevice(Path.Combine(_fasterPath, "hlog.log"));
        _fasterStore = new FasterKV<SpanByte, SpanByte>(
            1L << 20, // hash table size
            new LogSettings
            {
                LogDevice = _fasterLog,
                MemorySizeBits = fasterMemoryBits,
                PageSizeBits = 22   // 4MB pages for better throughput at scale
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

        // Clear ZoneTree - destroy and recreate
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

        // Clear FASTER - purge and recreate
        _fasterSession?.Dispose();
        _fasterStore?.Dispose();
        _fasterLog?.Dispose();
        if (Directory.Exists(_fasterPath))
        {
            try { Directory.Delete(_fasterPath, true); }
            catch { /* ignore locked files */ }
        }
        Directory.CreateDirectory(_fasterPath);
        var fasterMemoryBits = MessageCount switch
        {
            >= 1_000_000 => 30,
            >= 100_000 => 28,
            _ => 25
        };
        _fasterLog = Devices.CreateLogDevice(Path.Combine(_fasterPath, "hlog.log"));
        _fasterStore = new FasterKV<SpanByte, SpanByte>(
            1L << 20,
            new LogSettings
            {
                LogDevice = _fasterLog,
                MemorySizeBits = fasterMemoryBits,
                PageSizeBits = 22
            }
        );
        _fasterSession = _fasterStore.NewSession(new SpanByteFunctions<Empty>());
    }

    // ========================================================================
    // PUT BENCHMARKS - Simulate StoreIncoming (batch insert all messages)
    // ========================================================================

    [Benchmark(Description = "Put_LMDB")]
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
    }

    [Benchmark(Description = "Put_LMDB_Append")]
    [BenchmarkCategory("Put")]
    public void Put_Lmdb_Append()
    {
        using var tx = _lmdbEnv!.BeginTransaction();
        using var db = tx.OpenDatabase("test");
        for (var i = 0; i < MessageCount; i++)
        {
            tx.Put(db, _keys[i], _values[i], PutOptions.AppendData);
        }
        tx.Commit();
    }

    [Benchmark(Description = "Put_ZoneTree")]
    [BenchmarkCategory("Put")]
    public void Put_ZoneTree()
    {
        for (var i = 0; i < MessageCount; i++)
        {
            _zoneTree!.Upsert(_keys[i], _values[i]);
        }
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
    }

    // ========================================================================
    // GET BENCHMARKS - Simulate GetMessage (point lookup by key)
    // ========================================================================

    [Benchmark(Description = "Get_LMDB")]
    [BenchmarkCategory("Get")]
    public int Get_Lmdb()
    {
        // Setup: insert all data first
        using (var tx = _lmdbEnv!.BeginTransaction())
        {
            using var db = tx.OpenDatabase("test");
            for (var i = 0; i < MessageCount; i++)
                tx.Put(db, _keys[i], _values[i]);
            tx.Commit();
        }

        // Benchmark: read all by key
        var count = 0;
        using (var tx = _lmdbEnv!.BeginTransaction(TransactionBeginFlags.ReadOnly))
        {
            using var db = tx.OpenDatabase("test");
            for (var i = 0; i < MessageCount; i++)
            {
                var result = tx.Get(db, _keys[i]);
                if (result.resultCode == MDBResultCode.Success) count++;
            }
        }
        return count;
    }

    [Benchmark(Description = "Get_ZoneTree")]
    [BenchmarkCategory("Get")]
    public int Get_ZoneTree()
    {
        // Setup: insert all data first
        for (var i = 0; i < MessageCount; i++)
            _zoneTree!.Upsert(_keys[i], _values[i]);

        // Benchmark: read all by key
        var count = 0;
        for (var i = 0; i < MessageCount; i++)
        {
            if (_zoneTree!.TryGet(_keys[i], out _)) count++;
        }
        return count;
    }

    [Benchmark(Description = "Get_FASTER")]
    [BenchmarkCategory("Get")]
    public int Get_Faster()
    {
        // Setup: insert all data first
        for (var i = 0; i < MessageCount; i++)
        {
            var key = SpanByte.FromPinnedMemory(_keys[i]);
            var value = SpanByte.FromPinnedMemory(_values[i]);
            _fasterSession!.Upsert(ref key, ref value);
        }

        // Benchmark: read all by key
        var count = 0;
        for (var i = 0; i < MessageCount; i++)
        {
            var key = SpanByte.FromPinnedMemory(_keys[i]);
            var output = new SpanByteAndMemory();
            var status = _fasterSession!.Read(ref key, ref output);
            if (status.Found) count++;
            output.Memory?.Dispose();
        }
        return count;
    }

    // ========================================================================
    // ENUMERATE BENCHMARKS - Simulate PersistedIncoming (iterate all in order)
    // Note: FASTER does not support ordered iteration — this uses log scan
    // ========================================================================

    [Benchmark(Description = "Enumerate_LMDB")]
    [BenchmarkCategory("Enumerate")]
    public int Enumerate_Lmdb()
    {
        // Setup
        using (var tx = _lmdbEnv!.BeginTransaction())
        {
            using var db = tx.OpenDatabase("test");
            for (var i = 0; i < MessageCount; i++)
                tx.Put(db, _keys[i], _values[i]);
            tx.Commit();
        }

        // Benchmark: cursor enumeration (ordered)
        var count = 0;
        using (var tx = _lmdbEnv!.BeginTransaction(TransactionBeginFlags.ReadOnly))
        {
            using var db = tx.OpenDatabase("test");
            using var cursor = tx.CreateCursor(db);
            foreach (var _ in cursor.AsEnumerable())
            {
                count++;
            }
        }
        return count;
    }

    [Benchmark(Description = "Enumerate_ZoneTree")]
    [BenchmarkCategory("Enumerate")]
    public int Enumerate_ZoneTree()
    {
        // Setup
        for (var i = 0; i < MessageCount; i++)
            _zoneTree!.Upsert(_keys[i], _values[i]);

        // Benchmark: ordered iteration
        var count = 0;
        using var iterator = _zoneTree!.CreateIterator();
        while (iterator.Next())
        {
            count++;
        }
        return count;
    }

    [Benchmark(Description = "Enumerate_FASTER")]
    [BenchmarkCategory("Enumerate")]
    public int Enumerate_Faster()
    {
        // Setup
        for (var i = 0; i < MessageCount; i++)
        {
            var key = SpanByte.FromPinnedMemory(_keys[i]);
            var value = SpanByte.FromPinnedMemory(_values[i]);
            _fasterSession!.Upsert(ref key, ref value);
        }

        // Benchmark: log scan (NOT ordered - FASTER limitation)
        _fasterSession!.CompletePending(true);
        var count = 0;
        using var iter = _fasterStore!.Log.Scan(_fasterStore.Log.BeginAddress, _fasterStore.Log.TailAddress);
        while (iter.GetNext(out _))
        {
            count++;
        }
        return count;
    }

    // ========================================================================
    // DELETE BENCHMARKS - Simulate SuccessfullySent / DeleteIncoming
    // ========================================================================

    [Benchmark(Description = "Delete_LMDB")]
    [BenchmarkCategory("Delete")]
    public void Delete_Lmdb()
    {
        // Setup: insert
        using (var tx = _lmdbEnv!.BeginTransaction())
        {
            using var db = tx.OpenDatabase("test");
            for (var i = 0; i < MessageCount; i++)
                tx.Put(db, _keys[i], _values[i]);
            tx.Commit();
        }

        // Benchmark: delete all by key
        using (var tx = _lmdbEnv!.BeginTransaction())
        {
            using var db = tx.OpenDatabase("test");
            for (var i = 0; i < MessageCount; i++)
                tx.Delete(db, _keys[i]);
            tx.Commit();
        }
    }

    [Benchmark(Description = "Delete_ZoneTree")]
    [BenchmarkCategory("Delete")]
    public void Delete_ZoneTree()
    {
        // Setup: insert
        for (var i = 0; i < MessageCount; i++)
            _zoneTree!.Upsert(_keys[i], _values[i]);

        // Benchmark: delete all by key
        for (var i = 0; i < MessageCount; i++)
            _zoneTree!.ForceDelete(_keys[i]);
    }

    [Benchmark(Description = "Delete_FASTER")]
    [BenchmarkCategory("Delete")]
    public void Delete_Faster()
    {
        // Setup: insert
        for (var i = 0; i < MessageCount; i++)
        {
            var key = SpanByte.FromPinnedMemory(_keys[i]);
            var value = SpanByte.FromPinnedMemory(_values[i]);
            _fasterSession!.Upsert(ref key, ref value);
        }

        // Benchmark: delete all by key
        for (var i = 0; i < MessageCount; i++)
        {
            var key = SpanByte.FromPinnedMemory(_keys[i]);
            _fasterSession!.Delete(ref key);
        }
    }

    // ========================================================================
    // MIXED BENCHMARKS - Simulate full queue cycle: store → retrieve → delete
    // This is the most realistic benchmark for a message queue
    // ========================================================================

    [Benchmark(Description = "QueueCycle_LMDB")]
    [BenchmarkCategory("QueueCycle")]
    public int QueueCycle_Lmdb()
    {
        // 1. Store all messages
        using (var tx = _lmdbEnv!.BeginTransaction())
        {
            using var db = tx.OpenDatabase("test");
            for (var i = 0; i < MessageCount; i++)
                tx.Put(db, _keys[i], _values[i]);
            tx.Commit();
        }

        // 2. Read all via enumeration (like PersistedIncoming)
        var count = 0;
        using (var tx = _lmdbEnv!.BeginTransaction(TransactionBeginFlags.ReadOnly))
        {
            using var db = tx.OpenDatabase("test");
            using var cursor = tx.CreateCursor(db);
            foreach (var _ in cursor.AsEnumerable())
                count++;
        }

        // 3. Delete all (like SuccessfullyReceived)
        using (var tx = _lmdbEnv!.BeginTransaction())
        {
            using var db = tx.OpenDatabase("test");
            for (var i = 0; i < MessageCount; i++)
                tx.Delete(db, _keys[i]);
            tx.Commit();
        }

        return count;
    }

    [Benchmark(Description = "QueueCycle_LMDB_Append")]
    [BenchmarkCategory("QueueCycle")]
    public int QueueCycle_Lmdb_Append()
    {
        // 1. Store with AppendData (2x faster for ascending keys)
        using (var tx = _lmdbEnv!.BeginTransaction())
        {
            using var db = tx.OpenDatabase("test");
            for (var i = 0; i < MessageCount; i++)
                tx.Put(db, _keys[i], _values[i], PutOptions.AppendData);
            tx.Commit();
        }

        // 2. Enumerate
        var count = 0;
        using (var tx = _lmdbEnv!.BeginTransaction(TransactionBeginFlags.ReadOnly))
        {
            using var db = tx.OpenDatabase("test");
            using var cursor = tx.CreateCursor(db);
            foreach (var _ in cursor.AsEnumerable())
                count++;
        }

        // 3. Delete
        using (var tx = _lmdbEnv!.BeginTransaction())
        {
            using var db = tx.OpenDatabase("test");
            for (var i = 0; i < MessageCount; i++)
                tx.Delete(db, _keys[i]);
            tx.Commit();
        }

        return count;
    }

    [Benchmark(Description = "QueueCycle_ZoneTree")]
    [BenchmarkCategory("QueueCycle")]
    public int QueueCycle_ZoneTree()
    {
        // 1. Store all
        for (var i = 0; i < MessageCount; i++)
            _zoneTree!.Upsert(_keys[i], _values[i]);

        // 2. Enumerate (ordered)
        var count = 0;
        using (var iterator = _zoneTree!.CreateIterator())
        {
            while (iterator.Next())
                count++;
        }

        // 3. Delete all
        for (var i = 0; i < MessageCount; i++)
            _zoneTree!.ForceDelete(_keys[i]);

        return count;
    }

    [Benchmark(Description = "QueueCycle_FASTER")]
    [BenchmarkCategory("QueueCycle")]
    public int QueueCycle_Faster()
    {
        // 1. Store all
        for (var i = 0; i < MessageCount; i++)
        {
            var key = SpanByte.FromPinnedMemory(_keys[i]);
            var value = SpanByte.FromPinnedMemory(_values[i]);
            _fasterSession!.Upsert(ref key, ref value);
        }

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

        // 3. Delete all
        for (var i = 0; i < MessageCount; i++)
        {
            var key = SpanByte.FromPinnedMemory(_keys[i]);
            _fasterSession!.Delete(ref key);
        }

        return count;
    }
}

/// <summary>
/// Byte array comparer for ZoneTree that provides ascending order,
/// matching LMDB's default lexicographic byte comparison.
/// </summary>
public class ByteArrayComparerAscending : IRefComparer<Memory<byte>>
{
    public int Compare(in Memory<byte> x, in Memory<byte> y)
    {
        return x.Span.SequenceCompareTo(y.Span);
    }
}
