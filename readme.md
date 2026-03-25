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
        queue.Send(new Message
        {
            Data = "hello"u8.ToArray(),
            Id = MessageId.GenerateRandom(),
            Queue = "my-queue",
            Destination = new Uri("lq.tcp://localhost:5050")
        });
    }
}
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
    // Process the message and respond with one or more of the following:
    msg.QueueContext.SuccessfullyReceived();  // Done processing
    msg.QueueContext.ReceiveLater(TimeSpan.FromSeconds(1));  // Retry later
    msg.QueueContext.Enqueue(msg.Message);    // Re-enqueue to same/other queue
    msg.QueueContext.Send(msg.Message);       // Send to another endpoint
    msg.QueueContext.MoveTo("other-queue");   // Move to different queue
    msg.QueueContext.CommitChanges();         // Commit all changes atomically
}
```

---

## Architecture

### Pluggable Storage

The core library defines a storage-agnostic `IMessageStore` interface with `IStoreTransaction` for atomic operations.
Storage providers are distributed as separate NuGet packages, each implementing these interfaces.

**Currently available:**
- **LMDB** (`Bielu.PersistentQueues.Storage.LMDB`) — High-performance embedded key-value store powered by [LightningDB](https://github.com/CoreyKaylor/Lightning.NET).

**Extensibility:**
- Additional storage backends can be added by creating a package that references `Bielu.PersistentQueues` and implements `IMessageStore` and `IStoreTransaction`.

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

- **`lightningqueues.messages.sent`** - Counter: Total number of messages sent
- **`lightningqueues.messages.received`** - Counter: Total number of messages received
- **`lightningqueues.messages.enqueued`** - Counter: Total number of messages enqueued
- **`lightningqueues.operations.errors`** - Counter: Total number of operation errors
- **`lightningqueues.message.processing.duration`** - Histogram: Duration of message processing in milliseconds
- **`lightningqueues.batch.size`** - Histogram: Number of messages in batches

All metrics include relevant dimensions such as `queue.name`, `operation`, and `batch.size`.

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
