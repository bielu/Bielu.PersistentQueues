# Bielu.PersistentQueues - Fast Persistent Queues for .NET

---

[![CI](https://github.com/bielu/Bielu.PersistentQueues/actions/workflows/buildAndPublishPackage.yml/badge.svg)](https://github.com/bielu/Bielu.PersistentQueues/actions/workflows/buildAndPublishPackage.yml)
[![NuGet version](https://img.shields.io/nuget/v/Bielu.PersistentQueues.svg)](https://www.nuget.org/packages/Bielu.PersistentQueues/)
[![NuGet Downloads](https://img.shields.io/nuget/dt/Bielu.PersistentQueues.svg)](https://www.nuget.org/packages/Bielu.PersistentQueues/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Bielu.PersistentQueues is a high-performance, lightweight, **store-and-forward message queue** for .NET applications with
**pluggable storage backends**. It ensures fast and durable persistence for sending and receiving messages, making it an
excellent choice for lightweight and cross-platform message queuing needs.

> This project is a fork of [LightningQueues](https://github.com/LightningQueues/LightningQueues), originally created by [Corey Kaylor](https://github.com/CoreyKaylor). It extends the original with additional features, bug fixes, and alignment with the bielu package ecosystem.

---

## Why Bielu.PersistentQueues?

- **Simple API**: Easily interact with the message queue through an intuitive API.
- **Strongly-Typed Messages**: Send, enqueue, and receive messages as C# objects — serialization is handled automatically via DI.
- **Pluggable Storage**: Choose from multiple storage backends (LMDB included, more coming).
- **Microsoft DI Integration**: First-class support for `Microsoft.Extensions.DependencyInjection` with a fluent builder API.
- **No Administration**: Unlike MSMQ or other Server / Brokers, it requires zero administrative setup.
- **XCopy Deployable**: No complex installation; just copy and run.
- **Cross-Platform**: Works on Windows, macOS, and Linux.
- **Durable Storage**: High-performance reliable message storage.
- **TLS Encryption**: Optionally secure your transport layer. You have full control.
- **Batch Receive**: Efficiently receive and process messages in batches.

---

## Packages

| Package | Description | NuGet |
|---------|-------------|-------|
| `Bielu.PersistentQueues` | Core library with queue abstractions and DI support | [![NuGet](https://img.shields.io/nuget/v/Bielu.PersistentQueues.svg)](https://www.nuget.org/packages/Bielu.PersistentQueues/) |
| `Bielu.PersistentQueues.Storage.LMDB` | LMDB storage provider (using LightningDB) | [![NuGet](https://img.shields.io/nuget/v/Bielu.PersistentQueues.Storage.LMDB.svg)](https://www.nuget.org/packages/Bielu.PersistentQueues.Storage.LMDB/) |
| `Bielu.PersistentQueues.OpenTelemetry` | OpenTelemetry instrumentation for distributed tracing and metrics | [![NuGet](https://img.shields.io/nuget/v/Bielu.PersistentQueues.OpenTelemetry.svg)](https://www.nuget.org/packages/Bielu.PersistentQueues.OpenTelemetry/) |

---

## Installation

Install the core package and the storage provider you want to use:

```bash
dotnet add package Bielu.PersistentQueues
dotnet add package Bielu.PersistentQueues.Storage.LMDB
```

---

## Getting Started

### Using Microsoft Dependency Injection (Recommended)

The recommended way to configure Bielu.PersistentQueues is via the fluent DI builder API:

```csharp
using Bielu.PersistentQueues;
using Bielu.PersistentQueues.Storage.LMDB;

services.AddPersistentQueues(builder =>
{
    builder
        .AddLmdbStorage("C:\\path_to_your_queue_folder")
        .ListenOn(new IPEndPoint(IPAddress.Loopback, 5050))
        .CreateQueues("my-queue");
});
```

You can also customize the LMDB storage configuration:

```csharp
services.AddPersistentQueues(builder =>
{
    builder
        .AddLmdbStorage("C:\\path_to_your_queue_folder", config =>
        {
            config.EnvironmentConfiguration = new LightningDB.EnvironmentConfiguration
            {
                MaxDatabases = 10,
                MapSize = 1024 * 1024 * 500 // 500 MB
            };
            config.StorageOptions = LmdbStorageOptions.MaxThroughput();
        })
        .AutomaticEndpoint()
        .CreateQueues("queue-a", "queue-b");
});
```

Then inject `IQueue` wherever you need it:

```csharp
public class MyService(IQueue queue)
{
    public void SendMessage()
    {
        // Strongly-typed: serialization is handled automatically
        queue.Send(new OrderMessage("ORD-001", 99.99m, "USD"),
            destinationUri: "lq.tcp://localhost:5050",
            queueName: "my-queue");
    }

    public void EnqueueMessage()
    {
        // Enqueue a typed object for local processing
        queue.Enqueue(new OrderMessage("ORD-002", 49.99m, "EUR"),
            queueName: "my-queue");
    }
}
```

You can also send raw `Message` objects if you need full control:

```csharp
queue.Send(new Message
{
    Data = "hello"u8.ToArray(),
    Id = MessageId.GenerateRandom(),
    Queue = "my-queue",
    Destination = new Uri("lq.tcp://localhost:5050")
});
```

### Manual Configuration (Without DI)

You can also configure the queue manually using `QueueConfiguration`:

```csharp
using Bielu.PersistentQueues;

var queue = new QueueConfiguration()
         .WithDefaults()
         .StoreWithLmdb("C:\\path_to_your_queue_folder")
         .BuildAndStart("queue-name");
```

### Sending Messages

Send strongly-typed objects — the content serializer (configured via DI or defaulting to JSON) handles serialization automatically:

```csharp
// Strongly-typed: serialize and send a C# object
queue.Send(new OrderMessage("ORD-001", 99.99m, "USD"),
    destinationUri: "lq.tcp://localhost:port",
    queueName: "queue-name");

// Strongly-typed: enqueue for local processing
queue.Enqueue(new OrderMessage("ORD-002", 49.99m, "EUR"),
    queueName: "queue-name");
```

Or send raw `Message` objects when you need full control over serialization:

```csharp
var message = new Message
{
     Data = "hello"u8.ToArray(),
     Id = MessageId.GenerateRandom(),
     Queue = "queue-name",
     Destination = new Uri("lq.tcp://localhost:port")
};
queue.Send(message);
```

### Receiving Messages

```csharp
await foreach (var msg in queue.Receive("queue-name", token))
{
    // Deserialize the message payload to a strongly-typed object
    var order = msg.Message.GetContent<OrderMessage>();

    // Process the message and respond with one or more of the following:
    msg.QueueContext.SuccessfullyReceived();  // Done processing
    msg.QueueContext.ReceiveLater(TimeSpan.FromSeconds(1));  // Retry later
    msg.QueueContext.Enqueue(msg.Message);    // Re-enqueue to same/other queue
    msg.QueueContext.Send(msg.Message);       // Send to another endpoint
    msg.QueueContext.MoveTo("other-queue");   // Move to different queue

    // Strongly-typed context operations (serializer resolved from DI):
    msg.QueueContext.Enqueue(new FollowUpMessage(...), queueName: "follow-ups");
    msg.QueueContext.Send(new ResponseMessage(...), destinationUri: "lq.tcp://...");

    msg.QueueContext.CommitChanges();         // Commit all changes atomically
}
```

---

## Dead Letter Queue (DLQ)

Bielu.PersistentQueues has built-in support for a **dead letter queue**. When a message cannot be processed after a configurable number of attempts, it is automatically moved to the DLQ — or you can explicitly dead-letter it from your consumer code.

### How It Works

- There is a single, shared dead letter queue named `dead-letter`.
- The DLQ is created automatically when any queue is created (and DLQ is enabled).
- Dead-lettered messages carry an `original-queue` header so you always know where they came from.
- A **processing-attempts** counter is incremented on every `ReceiveLater` call.

### Message ID Persistence

**Message IDs remain constant throughout the entire message lifecycle**, including:
- Multiple `ReceiveLater` operations (retry/defer)
- Moving to the dead letter queue (automatic or explicit)
- Moving between queues with `MoveTo`
- Reading from storage multiple times

Each `MessageId` consists of two components:
- `SourceInstanceId`: Identifies the queue instance that generated the message
- `MessageIdentifier`: A unique GUID (using COMB algorithm for performance)

Both components are preserved across all operations, enabling reliable message tracking, deduplication, and correlation in distributed systems. This persistence is guaranteed by the immutable `Message` struct design.

### Automatic Dead-Lettering (MaxAttempts)

Set `maxAttempts` when creating a message. When `ReceiveLater` is called and the processing attempt count reaches the limit, the message is moved to the DLQ automatically:

```csharp
// Create a message that will be dead-lettered after 3 failed processing attempts
var message = Message.Create(
    data: Encoding.UTF8.GetBytes("order-data"),
    queue: "orders",
    maxAttempts: 3);
queue.Enqueue(message);

// Consumer — each ReceiveLater call increments the processing attempt counter.
// After 3 attempts, the message is auto-moved to "dead-letter".
await foreach (var ctx in queue.Receive("orders", cancellationToken: token))
{
    try
    {
        ProcessOrder(ctx.Message);
        ctx.QueueContext.SuccessfullyReceived();
    }
    catch
    {
        ctx.QueueContext.ReceiveLater(TimeSpan.FromSeconds(5));
    }
    ctx.QueueContext.CommitChanges();
}
```

### Explicit Dead-Lettering

You can also move a message to the DLQ manually:

```csharp
await foreach (var ctx in queue.Receive("orders", cancellationToken: token))
{
    if (IsPoisonMessage(ctx.Message))
    {
        ctx.QueueContext.MoveToDeadLetter();  // → "dead-letter" queue
        ctx.QueueContext.CommitChanges();
        continue;
    }
    // normal processing …
}
```

Batch consumers can dead-letter individual messages or the entire batch:

```csharp
await foreach (var batch in queue.ReceiveBatch("orders", maxMessages: 10, cancellationToken: token))
{
    var poison = batch.Messages.Where(IsPoisonMessage).ToArray();
    var good   = batch.Messages.Except(poison).ToArray();

    batch.MoveToDeadLetter(poison);     // dead-letter the bad ones
    batch.SuccessfullyReceived(good);   // acknowledge the good ones
    batch.CommitChanges();
}
```

You can also mix subset operations with batch-wide operations. Messages already processed by subset operations are automatically excluded from batch-wide calls:

```csharp
await foreach (var batch in queue.ReceiveBatch("orders", maxMessages: 10, cancellationToken: token))
{
    // Process some messages individually
    batch.ReceiveLater(new[] { msg1.Id.MessageIdentifier }, TimeSpan.FromMinutes(5));
    batch.MoveToDeadLetter(new[] { msg2.Id.MessageIdentifier });

    // Then successfully receive the rest of the batch
    // This will only affect messages not already processed above
    batch.SuccessfullyReceived();
    batch.CommitChanges();
}
```

### Inspecting DLQ Messages

```csharp
foreach (var msg in queue.Store.PersistedIncoming(DeadLetterConstants.QueueName))
{
    Console.WriteLine($"Original queue: {msg.OriginalQueue}");
    Console.WriteLine($"Attempts:       {msg.ProcessingAttempts}");
    Console.WriteLine($"Data:           {Encoding.UTF8.GetString(msg.Data.Span)}");
}
```

### Requeuing DLQ Messages

Once you've fixed the underlying issue, you can move all messages from the DLQ back to their original queues in a single call. Processing attempt counters are reset to zero:

```csharp
int count = queue.RequeueDeadLetterMessages();
Console.WriteLine($"Requeued {count} messages back to their original queues");
```

### Clearing DLQ Messages

If you want to permanently remove all messages from the DLQ without requeuing them:

```csharp
int count = queue.ClearDeadLetterQueue();
Console.WriteLine($"Permanently deleted {count} messages from the dead letter queue");
```

**Warning:** This operation permanently deletes all messages in the DLQ and cannot be undone. Use with caution.

### Enabling the DLQ

The DLQ is **disabled by default**. Enable it via the builder API:

**Using DI:**

```csharp
services.AddPersistentQueues(builder =>
{
    builder
        .AddLmdbStorage("./queue_data")
        .WithDeadLetterQueue()   // ← enable dead letter queue with default settings
        .CreateQueues("my-queue");
});
```

**Manual configuration:**

```csharp
var queue = new QueueConfiguration()
    .WithDefaults()
    .WithDeadLetterQueue()
    .StoreWithLmdb("./queue_data")
    .BuildAndStart("my-queue");
```

When the DLQ is not enabled:
- `ReceiveLater` never auto-moves messages to the DLQ, even when `MaxAttempts` is exceeded — the message is retried indefinitely.
- Outgoing messages that fail all send retries are silently discarded.
- Calling `MoveToDeadLetter()` throws `InvalidOperationException`.

### Global Max Attempts

In addition to per-message `maxAttempts`, you can configure a **global maximum** that applies to all messages:

```csharp
services.AddPersistentQueues(builder =>
{
    builder
        .AddLmdbStorage("./queue_data")
        .WithDeadLetterQueue(options =>
        {
            options.GlobalMaxAttemptsForMessage = 5;  // Default is 3
        })
        .CreateQueues("my-queue");
});
```

**How it works:**
- If a message has `maxAttempts` set, the **lower** of the two values takes precedence.
- Example: `GlobalMaxAttemptsForMessage = 5`, message has `maxAttempts: 10` → DLQ after 5 attempts.
- Example: `GlobalMaxAttemptsForMessage = 5`, message has `maxAttempts: 3` → DLQ after 3 attempts.
- Messages without `maxAttempts` are dead-lettered after reaching the global max.

### DLQ Metrics (OpenTelemetry)

When OpenTelemetry is enabled, the following DLQ-specific metrics are emitted:

| Metric | Type | Description |
|--------|------|-------------|
| `bielupersistentqueues.messages.dead_lettered` | Counter | Total dead-lettered messages (tags: `queue.name`, `reason`) |
| `bielupersistentqueues.dead_letter.queue.depth` | Gauge | Current message count in the DLQ |

Reason values: `manual`, `max_processing_attempts`, `send_failed`.

---

## Architecture

### Pluggable Storage

The core library defines a storage-agnostic `IMessageStore` interface with `IStoreTransaction` for atomic operations.
Storage providers are distributed as separate NuGet packages, each implementing these interfaces.

**Currently available:**
- **LMDB** (`Bielu.PersistentQueues.Storage.LMDB`) — High-performance embedded key-value store powered by [LightningDB](https://github.com/CoreyKaylor/Lightning.NET).
- **ZoneTree** (`Bielu.PersistentQueues.Storage.ZoneTree`) — Pure C# LSM-tree storage with no native dependencies.

**Extensibility:**
- Additional storage backends can be added by creating a package that references `Bielu.PersistentQueues` and implements `IMessageStore` and `IStoreTransaction`.

### Storage Provider Comparison

Two storage backends are available. Choose based on your requirements:

| Feature | LMDB | ZoneTree |
|---------|------|----------|
| Implementation | C wrapper (LightningDB) | Pure C# (LSM tree) |
| Native dependencies | Yes (lmdb) | None |
| Transaction model | Full ACID | Buffered commit |
| Memory mapping | Yes (mmap) | No |
| Best for | Low-latency point lookups | Write-heavy workloads |

#### Benchmark Results

Benchmarked on Linux (Ubuntu 24.04), AMD EPYC 7763, .NET 10.0.5.
Times are in microseconds (μs). Lower is better.

##### Small batches (100 messages)

| Operation | LMDB (64B) | ZoneTree (64B) | LMDB (512B) | ZoneTree (512B) |
|-----------|------------|----------------|-------------|-----------------|
| **StoreIncoming** | 308 μs | **171 μs** | 404 μs | **204 μs** |
| **GetMessage** | 655 μs | **506 μs** | 717 μs | **486 μs** |
| **PersistedIncoming** | **595 μs** | 608 μs | **948 μs** | 837 μs |
| **DeleteIncoming** | 808 μs | **446 μs** | 762 μs | **289 μs** |
| **QueueCycle** | 848 μs | **668 μs** | 966 μs | **656 μs** |
| **OutgoingCycle** | 24,987 μs | **828 μs** | 25,402 μs | **844 μs** |

##### Larger batches (1,000 messages)

| Operation | LMDB (64B) | ZoneTree (64B) | LMDB (512B) | ZoneTree (512B) |
|-----------|------------|----------------|-------------|-----------------|
| **StoreIncoming** | **1,541 μs** | 2,041 μs | **2,117 μs** | 2,149 μs |
| **GetMessage** | **4,406 μs** | 4,703 μs | **4,863 μs** | 5,060 μs |
| **PersistedIncoming** | **4,254 μs** | 5,161 μs | **4,390 μs** | 5,172 μs |
| **DeleteIncoming** | **2,871 μs** | 3,575 μs | **3,664 μs** | 3,766 μs |
| **QueueCycle** | **5,268 μs** | 6,719 μs | **6,028 μs** | 6,746 μs |
| **OutgoingCycle** | 261,570 μs | **8,298 μs** | 335,853 μs | **8,341 μs** |

##### Summary

- **ZoneTree excels** at small-batch operations (100 messages) where it is consistently faster across all operations, particularly for store and delete.
- **LMDB excels** at larger batches (1,000+ messages) where its transactional batch commit model amortizes overhead and its B+ tree provides excellent read locality.
- **LMDB has lower memory allocations** due to zero-copy reads from memory-mapped files.
- In the benchmarked outgoing message workflow using `PersistedOutgoing()` with per-message `SuccessfullySent()`, **ZoneTree dramatically outperforms LMDB** at all scales measured (30x faster at 1K messages).
- These outgoing-cycle results do **not** measure the production raw/bulk send path (`PersistedOutgoingRaw` + `SuccessfullySentByIds`), so they should not be treated as a universal conclusion for all send scenarios.
- Choose **LMDB** for read-heavy, large-batch, low-latency workloads with available native dependencies.
- Choose **ZoneTree** for write-heavy, small-batch workloads or when native dependencies are not available, especially when your usage matches the benchmarked API pattern above.

### Custom Storage Providers

To implement your own storage provider:

1. Create a new project referencing `Bielu.PersistentQueues`
2. Implement `IMessageStore` and `IStoreTransaction`
3. Create an extension method on `PersistentQueuesBuilder` to register your storage:

```csharp
public static PersistentQueuesBuilder AddMyStorage(
    this PersistentQueuesBuilder builder,
    string connectionString)
{
    builder.UseStorage(sp =>
    {
        // Create and return your IMessageStore implementation
        return new MyMessageStore(connectionString);
    });
    return builder;
}
```

---

## Strongly-Typed Messages

Bielu.PersistentQueues supports sending, enqueuing, and receiving **strongly-typed C# objects** as message payloads. Serialization is handled automatically — the content serializer is resolved from DI, so you never need to manually serialize or pass a serializer instance.

### Sending and Enqueuing Typed Objects

The `IQueue` and `IQueueContext` interfaces include `Send<T>` and `Enqueue<T>` methods that accept any object and serialize it using the DI-configured `IContentSerializer` (JSON by default):

```csharp
// Send a typed object to a remote endpoint
queue.Send(new OrderMessage("ORD-001", 99.99m, "USD"),
    destinationUri: "lq.tcp://localhost:5050",
    queueName: "orders");

// Enqueue a typed object for local processing
queue.Enqueue(new OrderMessage("ORD-002", 49.99m, "EUR"),
    queueName: "orders");
```

Inside a message handler, the same methods are available on `IQueueContext`:

```csharp
await foreach (var msg in queue.Receive("orders", token))
{
    var order = msg.Message.GetContent<OrderMessage>();

    // Enqueue a follow-up message (serializer from DI)
    msg.QueueContext.Enqueue(new AuditEvent(order.OrderId, "processed"),
        queueName: "audit");

    // Send a response to another endpoint
    msg.QueueContext.Send(new OrderConfirmation(order.OrderId),
        destinationUri: "lq.tcp://notifications:5050",
        queueName: "confirmations");

    msg.QueueContext.SuccessfullyReceived();
    msg.QueueContext.CommitChanges();
}
```

### Deserializing Message Content

Use `Message.GetContent<T>()` to deserialize the `Data` payload:

```csharp
var order = message.GetContent<OrderMessage>();
```

### Custom Content Serializer

The default serializer uses `System.Text.Json`. You can replace it globally by registering your own `IContentSerializer` before calling `AddPersistentQueues`:

```csharp
// Register a custom serializer (e.g., MessagePack, Protobuf)
services.AddSingleton<IContentSerializer, MyMessagePackSerializer>();

// Queue will automatically use it for all Send<T>/Enqueue<T>/GetContent<T> calls
services.AddPersistentQueues(builder => { ... });
```

For one-off overrides, extension methods accept an explicit serializer:

```csharp
var msgPackSerializer = new MyMessagePackSerializer();
queue.Send(order, msgPackSerializer, destinationUri: "lq.tcp://...", queueName: "orders");
queue.Enqueue(order, msgPackSerializer, queueName: "orders");
```

When using manual configuration (without DI), you can configure the serializer via the builder:

```csharp
var queue = new QueueConfiguration()
    .WithDefaults()
    .SerializeContentWith(new MyMessagePackSerializer())
    .StoreWithLmdb("C:\\queue_path")
    .BuildAndStart("queue-name");
```

---

## Running Tests

To ensure everything is running smoothly, clone the repository and run:

```bash
dotnet test src/Bielu.PersistentQueues.slnx
```

---

## OpenTelemetry Integration

Bielu.PersistentQueues provides built-in **OpenTelemetry** support for distributed tracing and metrics through the `PersistentQueueOtelDecorator`. This enables comprehensive observability of your queue operations.

### Installation

```bash
dotnet add package Bielu.PersistentQueues.OpenTelemetry
```

### Usage

Add instrumentation to your service collection to automatically decorate the queue with OpenTelemetry:

```csharp
using Bielu.PersistentQueues.OpenTelemetry;

services.AddBieluPersistentQueueInstrumentation();
```

### Metrics Collected

The decorator automatically collects the following metrics:

- **`bielupersistentqueues.messages.sent`** - Counter: Total number of messages sent
- **`bielupersistentqueues.messages.received`** - Counter: Total number of messages received
- **`bielupersistentqueues.messages.enqueued`** - Counter: Total number of messages enqueued
- **`bielupersistentqueues.operations.errors`** - Counter: Total number of operation errors
- **`bielupersistentqueues.message.processing.duration`** - Histogram: Duration of message processing in milliseconds
- **`bielupersistentqueues.batch.size`** - Histogram: Number of messages in batches
- **`bielupersistentqueues.queues.active`** - Gauge: Number of currently active queues
- **`bielupersistentqueues.storage.used_bytes`** - Gauge: Number of bytes currently used by the storage
- **`bielupersistentqueues.storage.total_bytes`** - Gauge: Total number of bytes allocated for the storage
- **`bielupersistentqueues.storage.usage_percent`** - Gauge: Percentage of storage currently in use (0–100%)

All metrics include relevant dimensions such as `queue.name`, `operation`, and `batch.size`.

> **Note:** Storage usage metrics are automatically registered when the underlying store supports usage reporting (e.g., LMDB). No additional configuration is needed beyond enabling OpenTelemetry instrumentation.

### Distributed Tracing

The decorator creates OpenTelemetry activities (spans) for all queue operations:

- **Producer operations**: `Send`, `SendBatch`, `Enqueue`
- **Consumer operations**: `Receive`, `ReceiveBatch`, `ProcessMessage`, `ProcessBatch`
- **Internal operations**: `CreateQueue`, `Start`, `MoveToQueue`, `ReceiveLater`

Each span includes tags such as:
- `queue.name` - The queue name
- `message.id` - Unique message identifier
- `destination` - Message destination endpoint
- `batch.size` - Number of messages in batch operations
- `delay.seconds` - Delay time for scheduled messages

### Error Tracking

All operations are wrapped with exception handling that:
- Records error metrics with operation context
- Sets activity status to `Error`
- Adds exception details to the span
- Preserves original exception for caller handling

### Example: Full Configuration with OpenTelemetry

```csharp
using OpenTelemetry;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;
using Bielu.PersistentQueues.OpenTelemetry;

// Configure OpenTelemetry
services.AddOpenTelemetry()
    .WithMetrics(metrics => metrics
        .AddBieluPersistentQueuesInstrumentation())
    .WithTracing(tracing => tracing
        .AddBieluPersistentQueuesInstrumentation());

// Configure queue
services.AddPersistentQueues(builder =>
{
    builder
        .AddLmdbStorage("C:\\queue_path")
        .ListenOn(new IPEndPoint(IPAddress.Loopback, 5050))
        .CreateQueues("my-queue");
});

// Add instrumentation
services.AddBieluPersistentQueueInstrumentation();
```

---

## Partitioned Queues (Kafka-like Partitioning)

Bielu.PersistentQueues supports **Kafka-like partitioning** to enable parallel consumption and message ordering guarantees within a partition key.

### Concepts

- A **partitioned queue** divides a logical queue into N **partitions** (e.g., `orders:partition-0`, `orders:partition-1`, ...).
- Messages are routed to partitions using a configurable **partition strategy**.
- Messages with the **same partition key** are guaranteed to land in the **same partition**, preserving ordering for related messages.
- Consumers can receive from **all partitions**, a **specific partition**, or a **subset of partitions**.

### Built-in Partition Strategies

| Strategy | Description |
|----------|-------------|
| `HashPartitionStrategy` (default) | Routes messages based on FNV-1a hash of the partition key. Same key → same partition. Messages without a key use the message ID for distribution. |
| `RoundRobinPartitionStrategy` | Distributes messages evenly across partitions in round-robin order, ignoring partition keys. |
| `ExplicitPartitionStrategy` | Routes messages to the partition index specified in the partition key (e.g., key `"2"` → partition 2). |

### Manual Configuration

```csharp
var queue = new QueueConfiguration()
    .WithDefaults()
    .StoreWithLmdb("C:\\queue_path", StorageSize.MB(100))
    .BuildAndStartPartitioned("orders", partitionCount: 4);

// Enqueue with a partition key (routed by strategy)
var message = Message.Create(
    data: "order-data"u8.ToArray(),
    queue: "orders",
    partitionKey: "customer-123"
);
queue.EnqueueToPartition(message, "orders");

// Receive from a specific partition
await foreach (var ctx in queue.ReceiveFromPartition("orders", partition: 0, cancellationToken: token))
{
    // Process message
    ctx.QueueContext.SuccessfullyReceived();
    ctx.QueueContext.CommitChanges();
}
```

### Using Dependency Injection

```csharp
services.AddPersistentQueues(builder =>
{
    builder
        .AddLmdbStorage("C:\\queue_path", config =>
        {
            config.MapSize = StorageSize.MB(500);  // Use StorageSize helper instead of raw bytes
            config.EnvironmentConfiguration.MaxDatabases = 20;  // Increase for partitioned queues
        })
        .AutomaticEndpoint()
        .UsePartitioning(
            new HashPartitionStrategy(),
            ("orders", 4),       // 4 partitions for "orders"
            ("events", 8)        // 8 partitions for "events"
        );
});

// Inject IPartitionedQueue
public class MyService(IPartitionedQueue queue)
{
    public void ProcessOrder(string customerId, byte[] data)
    {
        var message = Message.Create(
            data: data,
            queue: "orders",
            partitionKey: customerId  // Same customer → same partition → ordered
        );
        queue.EnqueueToPartition(message, "orders");
    }

    public async Task ConsumePartition(int partition, CancellationToken token)
    {
        await foreach (var ctx in queue.ReceiveFromPartition("orders", partition, cancellationToken: token))
        {
            // Deserialize to a strongly-typed object
            var order = ctx.Message.GetContent<OrderMessage>();

            // Process — messages from same partition key arrive in order
            ctx.QueueContext.SuccessfullyReceived();
            ctx.QueueContext.CommitChanges();
        }
    }
}
```

### Custom Partition Strategy

Implement `IPartitionStrategy` to create your own routing logic:

```csharp
public class GeoPartitionStrategy : IPartitionStrategy
{
    public int GetPartition(Message message, int partitionCount)
    {
        // Route by geographic region from partition key
        var region = message.PartitionKeyString;
        return region switch
        {
            "us-east" => 0 % partitionCount,
            "us-west" => 1 % partitionCount,
            "eu" => 2 % partitionCount,
            _ => 0
        };
    }
}
```

### LMDB Configuration Note

When using partitioned queues with LMDB storage, ensure `MaxDatabases` is set high enough to accommodate all partitions. Each partition creates a separate LMDB database. For example, 4 partitions + 1 outgoing database requires at least `MaxDatabases = 6`.

---

## Storage Usage Monitoring

Bielu.PersistentQueues exposes storage usage information through the `IMessageStore.GetStorageUsageInfo()` API. This is useful for monitoring disk pressure and alerting when storage is nearing capacity.

### Programmatic Access

You can query storage usage directly from the store:

```csharp
var usageInfo = queue.Store.GetStorageUsageInfo();
if (usageInfo != null)
{
    Console.WriteLine($"Used:  {usageInfo.Value.UsedBytes} bytes");
    Console.WriteLine($"Total: {usageInfo.Value.TotalBytes} bytes");
    Console.WriteLine($"Usage: {usageInfo.Value.UsagePercentage:F2}%");
}
```

> `GetStorageUsageInfo()` returns `null` if the storage provider does not support usage reporting. The LMDB provider reports usage based on the environment's page utilization relative to the configured `MapSize`.

### OpenTelemetry Integration

When OpenTelemetry instrumentation is enabled, storage usage metrics are automatically collected as observable gauges — no extra configuration is needed:

```csharp
services.AddPersistentQueues(builder =>
{
    builder
        .AddLmdbStorage("./queue_data", config =>
        {
            config.MapSize = StorageSize.MB(100);  // Use StorageSize helper
        })
        .CreateQueues("my-queue");
});

// Storage gauges are registered automatically
services.AddBieluPersistentQueueInstrumentation();
```

The three storage gauges (`storage.used_bytes`, `storage.total_bytes`, `storage.usage_percent`) are polled at each metric scrape and always reflect the current state.

---

## Transport Security (TLS Encryption)

Bielu.PersistentQueues supports **TLS encryption** to secure communication. The library provides hooks to enable custom
certificate validation and encryption settings:

```csharp
services.AddPersistentQueues(builder =>
{
    builder
        .AddLmdbStorage("C:\\queue_path")
        .SecureTransportWith(
            new TlsStreamSecurity(async (uri, stream) =>
            {
                var sslStream = new SslStream(stream, true, (_, _, _, _) => true, null);
                await sslStream.AuthenticateAsClientAsync(uri.Host);
                return sslStream;
            }),
            new TlsStreamSecurity(async (_, stream) =>
            {
                var sslStream = new SslStream(stream, false);
                await sslStream.AuthenticateAsServerAsync(certificate, false,
                    checkCertificateRevocation: false, enabledSslProtocols: SslProtocols.Tls12);
                return sslStream;
            }));
});
```

---

## Contributing

We welcome contributions! See our [Contributing Guide](./CONTRIBUTING.md) for details on:
- Setting up your development environment
- Building and testing the project
- Submitting pull requests

---

## Acknowledgments

This project is based on and builds upon the excellent work of:

- **[LightningQueues](https://github.com/LightningQueues/LightningQueues)** - The original fast persistent queues library for .NET, created and maintained by [Corey Kaylor](https://github.com/CoreyKaylor) and contributors. Licensed under the MIT License.

---

## License

This project is licensed under the MIT License - see the [LICENSE](./LICENSE) file for details.
