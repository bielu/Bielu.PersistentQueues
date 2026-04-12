# Bielu.PersistentQueues

Fast persistent queues for .NET with pluggable storage backends.

## Features

- **Persistent messaging** — Messages survive process restarts
- **Pluggable storage** — Choose between LMDB (default) and ZoneTree backends
- **Partitioned queues** — Kafka-like partition semantics with exclusive consumer access
- **Dead letter queues** — Automatic DLQ support with requeue capability
- **Batch operations** — Efficient batch send/receive/acknowledge
- **OpenTelemetry** — Built-in metrics and tracing instrumentation
- **Clustering** — Optional distributed replication with RF=2

## Quick Start

```csharp
// Using LMDB (default, recommended for most workloads)
var queue = new QueueConfiguration()
    .WithDefaults()
    .StoreWithLmdb("/data/queues")
    .BuildQueue();

// Using ZoneTree (pure C#, no native dependencies)
var queue = new QueueConfiguration()
    .WithDefaults()
    .StoreWithZoneTree("/data/queues")
    .BuildQueue();

// Send a message
queue.CreateQueue("orders");
var message = Message.Create(data: payload, queue: "orders");
queue.Enqueue(message);

// Receive messages
await foreach (var ctx in queue.Receive("orders"))
{
    // Process message
    ctx.QueueContext.SuccessfullyReceived();
    ctx.QueueContext.CommitChanges();
}
```

### Dependency Injection

```csharp
// LMDB storage
services.AddPersistentQueues(builder => builder.AddLmdbStorage("/data/queues"));

// ZoneTree storage
services.AddPersistentQueues(builder => builder.AddZoneTreeStorage("/data/queues"));
```

## Storage Provider Comparison

Two storage backends are available. Choose based on your requirements:

| Feature | LMDB | ZoneTree |
|---------|------|----------|
| Implementation | C wrapper (LightningDB) | Pure C# (LSM tree) |
| Native dependencies | Yes (lmdb) | None |
| Transaction model | Full ACID | Buffered commit |
| Memory mapping | Yes (mmap) | No |
| Best for | Low-latency point lookups | Write-heavy workloads |

### Benchmark Results

Benchmarked on Linux (Ubuntu 24.04), AMD EPYC 7763, .NET 10.0.5.
Times are in microseconds (μs). Lower is better.

#### Small batches (100 messages)

| Operation | LMDB (64B) | ZoneTree (64B) | LMDB (512B) | ZoneTree (512B) |
|-----------|------------|----------------|-------------|-----------------|
| **StoreIncoming** | 308 μs | **171 μs** | 404 μs | **204 μs** |
| **GetMessage** | 655 μs | **506 μs** | 717 μs | **486 μs** |
| **PersistedIncoming** | **595 μs** | 608 μs | **948 μs** | 837 μs |
| **DeleteIncoming** | 808 μs | **446 μs** | 762 μs | **289 μs** |
| **QueueCycle** | 848 μs | **668 μs** | 966 μs | **656 μs** |
| **OutgoingCycle** | 24,987 μs | **828 μs** | 25,402 μs | **844 μs** |

#### Larger batches (1,000 messages)

| Operation | LMDB (64B) | ZoneTree (64B) | LMDB (512B) | ZoneTree (512B) |
|-----------|------------|----------------|-------------|-----------------|
| **StoreIncoming** | **1,541 μs** | 2,041 μs | 2,117 μs | **2,149 μs** |
| **GetMessage** | **4,406 μs** | 4,703 μs | **4,863 μs** | 5,060 μs |
| **PersistedIncoming** | **4,254 μs** | 5,161 μs | **4,390 μs** | 5,172 μs |
| **DeleteIncoming** | **2,871 μs** | 3,575 μs | **3,664 μs** | 3,766 μs |
| **QueueCycle** | **5,268 μs** | 6,719 μs | **6,028 μs** | 6,746 μs |
| **OutgoingCycle** | 261,570 μs | **8,298 μs** | 335,853 μs | **8,341 μs** |

#### Memory allocation

| Operation (100 msgs) | LMDB (64B) | ZoneTree (64B) | LMDB (512B) | ZoneTree (512B) |
|----------------------|------------|----------------|-------------|-----------------|
| StoreIncoming | **64 KB** | 96 KB | **64 KB** | 139 KB |
| QueueCycle | **148 KB** | 273 KB | **192 KB** | 312 KB |
| OutgoingCycle | **162 KB** | 236 KB | **250 KB** | 368 KB |

#### Summary

- **ZoneTree excels** at small-batch operations (100 messages) where it is consistently faster across all operations, particularly for store and delete.
- **LMDB excels** at larger batches (1,000+ messages) where its transactional batch commit model amortizes overhead and its B+ tree provides excellent read locality.
- **LMDB has lower memory allocations** due to zero-copy reads from memory-mapped files.
- **ZoneTree dramatically outperforms LMDB** in the outgoing message cycle at all scales (30x faster at 1K messages) due to its LSM-tree write optimization.
- Choose **LMDB** for read-heavy, large-batch, low-latency workloads with available native dependencies.
- Choose **ZoneTree** for write-heavy, small-batch workloads or when native dependencies are not available.

### Running Benchmarks

```bash
cd src/Bielu.PersistentQueues.Benchmarks

# Compare storage providers at IMessageStore level
dotnet run -c Release -- --filter "*MessageStoreBenchmark*"

# Raw storage engine comparison (LMDB vs ZoneTree vs FASTER)
dotnet run -c Release -- --filter "*StorageProviderBenchmark*"

# LMDB-specific optimizations
dotnet run -c Release -- --filter "*LmdbStorageBenchmark*"

# Regression suite (CI/CD)
dotnet run -c Release -- --filter "*RegressionBenchmark*"
```

## Projects

| Project | Description |
|---------|-------------|
| `Bielu.PersistentQueues` | Core library — queue interfaces, serialization, partitioning |
| `Bielu.PersistentQueues.Storage.LMDB` | LMDB storage backend |
| `Bielu.PersistentQueues.Storage.ZoneTree` | ZoneTree storage backend |
| `Bielu.PersistentQueues.OpenTelemetry` | OpenTelemetry metrics & tracing |
| `Bielu.PersistentQueues.Cluster` | Distributed clustering with RF=2 replication |
| `Bielu.PersistentQueues.Benchmarks` | BenchmarkDotNet performance suite |

## License

See [LICENSE](LICENSE) for details.
