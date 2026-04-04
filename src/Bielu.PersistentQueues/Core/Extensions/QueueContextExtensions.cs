using System;
using System.Collections.Generic;
using Bielu.PersistentQueues.Serialization;

namespace Bielu.PersistentQueues;

/// <summary>
/// Extension methods for <see cref="IQueueContext"/> that provide strongly-typed message operations.
/// </summary>
public static class QueueContextExtensions
{
    /// <summary>
    /// Sends a strongly-typed content object to its destination from within a queue context.
    /// The content is serialized to a message using the specified or default content serializer.
    /// </summary>
    /// <typeparam name="T">The type of the content to send.</typeparam>
    /// <param name="context">The queue context.</param>
    /// <param name="content">The content object to serialize and send.</param>
    /// <param name="destinationUri">The destination URI (e.g., "lq.tcp://hostname:port").</param>
    /// <param name="queueName">Optional queue name for the message.</param>
    /// <param name="contentSerializer">The content serializer to use. If null, <see cref="JsonContentSerializer.Default"/> is used.</param>
    /// <param name="headers">Optional message headers.</param>
    /// <param name="deliverBy">Optional delivery deadline.</param>
    /// <param name="maxAttempts">Optional maximum delivery attempts.</param>
    /// <param name="partitionKey">Optional partition key.</param>
    public static void Send<T>(
        this IQueueContext context,
        T content,
        string? destinationUri = null,
        string? queueName = null,
        IContentSerializer? contentSerializer = null,
        Dictionary<string, string>? headers = null,
        DateTime? deliverBy = null,
        int? maxAttempts = null,
        string? partitionKey = null)
    {
        var message = Message.Create(
            content,
            contentSerializer: contentSerializer,
            queue: queueName,
            destinationUri: destinationUri,
            deliverBy: deliverBy,
            maxAttempts: maxAttempts,
            headers: headers,
            partitionKey: partitionKey);
        context.Send(message);
    }

    /// <summary>
    /// Adds a strongly-typed content object to the current queue from within a queue context.
    /// The content is serialized to a message using the specified or default content serializer.
    /// </summary>
    /// <typeparam name="T">The type of the content to enqueue.</typeparam>
    /// <param name="context">The queue context.</param>
    /// <param name="content">The content object to serialize and enqueue.</param>
    /// <param name="queueName">Optional queue name. If null, the message is enqueued without a specific queue name.</param>
    /// <param name="contentSerializer">The content serializer to use. If null, <see cref="JsonContentSerializer.Default"/> is used.</param>
    /// <param name="headers">Optional message headers.</param>
    /// <param name="partitionKey">Optional partition key.</param>
    public static void Enqueue<T>(
        this IQueueContext context,
        T content,
        string? queueName = null,
        IContentSerializer? contentSerializer = null,
        Dictionary<string, string>? headers = null,
        string? partitionKey = null)
    {
        var message = Message.Create(
            content,
            contentSerializer: contentSerializer,
            queue: queueName,
            headers: headers,
            partitionKey: partitionKey);
        context.Enqueue(message);
    }
}
