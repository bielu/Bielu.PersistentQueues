using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Bielu.PersistentQueues.Serialization;
using Bielu.PersistentQueues.Storage;

namespace Bielu.PersistentQueues;

/// <summary>
/// Defines the interface for a message queue that can send and receive messages across the network.
/// </summary>
public interface IQueue : IDisposable, IAsyncDisposable
{
    /// <summary>
    /// Gets the message store used by this queue for persistence.
    /// </summary>
    public IMessageStore Store { get; }

    /// <summary>
    /// Gets an array of all queue names available in this queue instance.
    /// </summary>
    public string[] Queues { get; }

    /// <summary>
    /// Gets the network endpoint where this queue is listening for incoming messages.
    /// </summary>
    public IPEndPoint Endpoint { get; }

    /// <summary>
    /// Creates a new queue with the specified name.
    /// </summary>
    /// <param name="queueName">The name of the queue to create.</param>
    public void CreateQueue(string queueName);

    /// <summary>
    /// Starts the queue's processing operations.
    /// </summary>
    public void Start();

    /// <summary>
    /// Receives messages from the specified queue as an asynchronous stream.
    /// </summary>
    /// <param name="queueName">The name of the queue to receive messages from.</param>
    /// <param name="pollIntervalInMilliseconds">The period to rest before checking for new messages if no messages are found.</param>
    /// <param name="cancellationToken">A token to cancel the receive operation.</param>
    /// <returns>An asynchronous stream of <see cref="IMessageContext"/> objects.</returns>
    public IAsyncEnumerable<IMessageContext> Receive(string queueName, int pollIntervalInMilliseconds = 200,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Receives batches of messages from the specified queue as an asynchronous stream.
    /// </summary>
    /// <param name="queueName">The name of the queue to receive messages from.</param>
    /// <param name="maxMessages">The maximum number of messages per batch. When zero or negative, all available messages in each poll cycle are returned.</param>
    /// <param name="batchTimeoutInMilliseconds">
    /// Time in milliseconds to keep collecting messages before yielding a batch.
    /// Acts as an <b>alternative</b> to <paramref name="maxMessages"/>: a batch is yielded
    /// when either the timeout elapses or <paramref name="maxMessages"/> is reached,
    /// whichever comes first. When used alone (without <paramref name="maxMessages"/>),
    /// the method waits for the full timeout period and then yields all messages that
    /// arrived during that window. When zero or negative, the timeout is disabled and
    /// batches are yielded as soon as messages are available.
    /// </param>
    /// <param name="pollIntervalInMilliseconds">The period to rest before checking for new messages if no messages are found.</param>
    /// <param name="cancellationToken">A token to cancel the receive operation.</param>
    /// <returns>An asynchronous stream of <see cref="IBatchQueueContext"/> objects, each containing all messages found in a single batch cycle.</returns>
    public IAsyncEnumerable<IBatchQueueContext> ReceiveBatch(string queueName,
        int maxMessages = 0, int batchTimeoutInMilliseconds = 0, int pollIntervalInMilliseconds = 200,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Schedules a message to be available for processing after a specified delay.
    /// </summary>
    /// <param name="message">The message to delay.</param>
    /// <param name="timeSpan">The time to delay processing of the message.</param>
    public void ReceiveLater(Message message, TimeSpan timeSpan);

    /// <summary>
    /// Schedules a message to be available for processing at a specific time.
    /// </summary>
    /// <param name="message">The message to delay.</param>
    /// <param name="time">The time when the message should become available.</param>
    public void ReceiveLater(Message message, DateTimeOffset time);

    /// <summary>
    /// Moves a message from its current queue to another queue.
    /// </summary>
    /// <param name="queueName">The name of the target queue.</param>
    /// <param name="message">The message to move.</param>
    public void MoveToQueue(string queueName, Message message);

    /// <summary>
    /// Sends multiple messages to their respective destinations.
    /// </summary>
    /// <param name="messages">An array of messages to send.</param>
    public void Send(params Message[] messages);

    /// <summary>
    /// Sends a single message to its destination.
    /// </summary>
    /// <param name="message">The message to send.</param>
    public void Send(Message message);

    /// <summary>
    /// Sends a strongly-typed content object to its destination.
    /// The content is serialized using the queue's configured content serializer.
    /// </summary>
    /// <typeparam name="T">The type of the content to send.</typeparam>
    /// <param name="content">The content object to serialize and send.</param>
    /// <param name="destinationUri">The destination URI (e.g., "lq.tcp://hostname:port").</param>
    /// <param name="queueName">Optional queue name for the message.</param>
    /// <param name="headers">Optional message headers.</param>
    /// <param name="deliverBy">Optional delivery deadline.</param>
    /// <param name="maxAttempts">Optional maximum delivery attempts.</param>
    /// <param name="partitionKey">Optional partition key.</param>
    public void Send<T>(
        T content,
        string? destinationUri = null,
        string? queueName = null,
        Dictionary<string, string>? headers = null,
        DateTime? deliverBy = null,
        int? maxAttempts = null,
        string? partitionKey = null);

    /// <summary>
    /// Adds a message directly to a queue for local processing.
    /// </summary>
    /// <param name="message">The message to enqueue.</param>
    public void Enqueue(Message message);

    /// <summary>
    /// Adds a strongly-typed content object directly to a queue for local processing.
    /// The content is serialized using the queue's configured content serializer.
    /// </summary>
    /// <typeparam name="T">The type of the content to enqueue.</typeparam>
    /// <param name="content">The content object to serialize and enqueue.</param>
    /// <param name="queueName">Optional queue name.</param>
    /// <param name="headers">Optional message headers.</param>
    /// <param name="partitionKey">Optional partition key.</param>
    public void Enqueue<T>(
        T content,
        string? queueName = null,
        Dictionary<string, string>? headers = null,
        string? partitionKey = null);

    /// <summary>
    /// Moves all messages from the dead letter queue back to their original source queues.
    /// </summary>
    /// <param name="deadLetterQueueName">
    /// The name of the dead letter queue (must be <see cref="DeadLetterConstants.QueueName"/>).
    /// </param>
    /// <returns>The number of messages that were requeued.</returns>
    /// <exception cref="ArgumentException">
    /// Thrown when <paramref name="deadLetterQueueName"/> does not match the shared dead letter queue name.
    /// </exception>
    /// <remarks>
    /// Each message is moved back to the queue recorded in its <c>original-queue</c> header.
    /// The processing attempt counter is reset to zero so the message can be retried.
    /// All moves happen in a single atomic transaction.
    /// </remarks>
    int RequeueDeadLetterMessages(string deadLetterQueueName);
}