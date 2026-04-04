using System;
using System.Collections.Generic;
using Bielu.PersistentQueues.Serialization;

namespace Bielu.PersistentQueues;

/// <summary>
/// Defines the operations available for processing a message in a queue.
/// </summary>
/// <remarks>
/// IQueueContext provides methods for handling messages during processing,
/// such as committing changes, sending responses, deferring processing,
/// or moving messages to different queues.
/// </remarks>
public interface IQueueContext
{
    /// <summary>
    /// Commits any pending changes to the message.
    /// </summary>
    /// <remarks>
    /// This method persists any modifications made to the message during processing.
    /// It should be called after successful message processing to ensure changes are saved.
    /// </remarks>
    void CommitChanges();
    
    /// <summary>
    /// Sends a message to its specified destination.
    /// </summary>
    /// <param name="message">The message to send.</param>
    /// <remarks>
    /// This method is typically used to send a response or follow-up message
    /// after processing the current message.
    /// </remarks>
    void Send(Message message);

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
    void Send<T>(
        T content,
        string? destinationUri = null,
        string? queueName = null,
        Dictionary<string, string>? headers = null,
        DateTime? deliverBy = null,
        int? maxAttempts = null,
        string? partitionKey = null);

    /// <summary>
    /// Schedules the current message to be processed again after a specified delay.
    /// </summary>
    /// <param name="timeSpan">The time to wait before making the message available again.</param>
    /// <remarks>
    /// This is useful for implementing retry logic or delayed processing.
    /// </remarks>
    void ReceiveLater(TimeSpan timeSpan);
    
    /// <summary>
    /// Schedules the current message to be processed again at a specific time.
    /// </summary>
    /// <param name="time">The time at which the message should be made available again.</param>
    /// <remarks>
    /// This is useful for scheduling message processing at a specific point in time.
    /// </remarks>
    void ReceiveLater(DateTimeOffset time);
    
    /// <summary>
    /// Marks the current message as successfully received and processed.
    /// </summary>
    /// <remarks>
    /// This method removes the message from the queue after successful processing.
    /// It should be called when message processing is complete and the message
    /// no longer needs to be kept in the queue.
    /// </remarks>
    void SuccessfullyReceived();
    
    /// <summary>
    /// Moves the current message to a different queue.
    /// </summary>
    /// <param name="queueName">The name of the destination queue.</param>
    /// <remarks>
    /// This is useful for implementing workflow stages or error handling,
    /// where messages need to be routed to different queues based on processing results.
    /// </remarks>
    void MoveTo(string queueName);

    /// <summary>
    /// Moves the current message to the dead letter queue.
    /// </summary>
    /// <remarks>
    /// The message is moved to the shared <c>dead-letter</c> queue. The dead letter queue is
    /// created automatically when the queue is first set up (if DLQ is enabled).
    /// The message's <c>original-queue</c> header is stamped with the source queue name.
    /// Use this method when a message cannot be processed and should not be retried.
    /// </remarks>
    void MoveToDeadLetter();
    
    /// <summary>
    /// Adds a new message to the current queue.
    /// </summary>
    /// <param name="message">The message to enqueue.</param>
    /// <remarks>
    /// This method is used to add a new message directly to the queue
    /// for local processing, without sending it over the network.
    /// </remarks>
    void Enqueue(Message message);

    /// <summary>
    /// Adds a strongly-typed content object to the current queue.
    /// The content is serialized using the queue's configured content serializer.
    /// </summary>
    /// <typeparam name="T">The type of the content to enqueue.</typeparam>
    /// <param name="content">The content object to serialize and enqueue.</param>
    /// <param name="queueName">Optional queue name.</param>
    /// <param name="headers">Optional message headers.</param>
    /// <param name="partitionKey">Optional partition key.</param>
    void Enqueue<T>(
        T content,
        string? queueName = null,
        Dictionary<string, string>? headers = null,
        string? partitionKey = null);
}