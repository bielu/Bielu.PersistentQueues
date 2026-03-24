# Bielu.PersistentQueues - Fast Persistent Queues for .NET

---

[![CI](https://github.com/bielu/Bielu.PersistentQueues/actions/workflows/buildAndPublishPackage.yml/badge.svg)](https://github.com/bielu/Bielu.PersistentQueues/actions/workflows/buildAndPublishPackage.yml)
[![NuGet version](https://img.shields.io/nuget/v/Bielu.PersistentQueues.svg)](https://www.nuget.org/packages/Bielu.PersistentQueues/)
[![NuGet Downloads](https://img.shields.io/nuget/dt/Bielu.PersistentQueues.svg)](https://www.nuget.org/packages/Bielu.PersistentQueues/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Bielu.PersistentQueues is a high-performance, lightweight, **store-and-forward message queue** for .NET applications. Powered
by **LightningDB** (LMDB), it ensures fast and durable persistence for sending and receiving messages, making it an
excellent choice for lightweight and cross-platform message queuing needs.

> This project is a fork of [LightningQueues](https://github.com/LightningQueues/LightningQueues), originally created by [Corey Kaylor](https://github.com/CoreyKaylor). It extends the original with additional features, bug fixes, and alignment with the bielu package ecosystem.

---

## Why Bielu.PersistentQueues?

- **Simple API**: Easily interact with the message queue through an intuitive API.
- **No Administration**: Unlike MSMQ or other Server / Brokers, it requires zero administrative setup.
- **XCopy Deployable**: No complex installation; just copy and run.
- **Cross-Platform**: Works on Windows, macOS, and Linux.
- **Durable Storage**: Leverages LMDB for high-performance reliable message storage.
- **TLS Encryption**: Optionally secure your transport layer. You have full control.
- **Batch Receive**: Efficiently receive and process messages in batches.

---

## Installation

To use Bielu.PersistentQueues, add it to your .NET project via NuGet:

```bash
dotnet add package Bielu.PersistentQueues
```

---

## Getting Started

Here’s how to use the library to set up a message queue and send a message:

### 1. Creating a Queue

```csharp
using Bielu.PersistentQueues;

// Define queue location and create the queue
var queue = new QueueConfiguration()
         .WithDefaults("C:\\path_to_your_queue_folder")
         .BuildAndStart("queue-name");
```

### 2. Sending Messages

```csharp
// Send a message to the queue
var message = new Message
{
     Data = "hello"u8.ToArray(),
     Id = MessageId.GenerateRandom(), //source identifier (for the server instance) + message identifier
     Queue = "queue-name",
     Destination = new Uri("lq.tcp://localhost:port")
     //Note the uri pattern, can be DNS, loopback, etc.
};
queue.Send(message);
```

### 3. Receiving Messages

```csharp
// Start receiving messages asynchronously with IAsyncEnumerable<MessageContext>
var messages = queue.Receive("queue-name", token);
await foreach (var msg in messages)
{
    //process the message and respond with one or more of the following
    msg.QueueContext.ReceiveLater(TimeSpan.FromSeconds(1));
    msg.QueueContext.SuccessfullyReceived(); //nothing more to do, done processing
    msg.QueueContext.Enqueue(msg.Message); //ideally a new message enqueued to the queue name on the msg
    msg.QueueContext.Send(msg.Message); //send a response or send a message to another uri;
    msg.QueueContext.MoveTo("different-queue"); //moves the currently received message to a different queue
    msg.QueueContext.CommitChanges(); // Everything previous is gathered in memory and committed in one transaction with LightningDB
}
```

---

## Running Tests

To ensure everything is running smoothly, clone the repository and run:

```bash
dotnet test src/Bielu.PersistentQueues.slnx
```

---

## Transport Security (TLS Encryption)

Bielu.PersistentQueues supports **TLS encryption** to secure communication. The library provides hooks to enable custom
certificate validation and encryption settings. For example:

```csharp
var certificate = LoadYourCertificate();
configuration.SecureTransportWith(new TlsStreamSecurity(async (uri, stream) =>
{
    //client side with no validation of server certificate
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
```

You can customize the encryption level based on your requirements.

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