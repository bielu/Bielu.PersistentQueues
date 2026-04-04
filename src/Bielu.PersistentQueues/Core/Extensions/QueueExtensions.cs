using System;
using System.Collections.Generic;
using Bielu.PersistentQueues.Serialization;

namespace Bielu.PersistentQueues;

/// <summary>
/// Extension methods for <see cref="IQueue"/> that provide strongly-typed message operations
/// with an explicit content serializer override.
/// </summary>
/// <remarks>
/// For most use cases, prefer the built-in <see cref="IQueue.Send{T}"/> and
/// <see cref="IQueue.Enqueue{T}"/> methods which use the DI-configured serializer.
/// These extension methods are only needed when you want to use a different serializer
/// than the one configured in the queue.
/// </remarks>
public static class QueueExtensions
{
    /// <summary>
    /// Enqueues a strongly-typed content object using a specific content serializer.
    /// </summary>
    public static void Enqueue<T>(
        this IQueue queue,
        T content,
        IContentSerializer contentSerializer,
        string? queueName = null,
        Dictionary<string, string>? headers = null,
        string? partitionKey = null)
    {
        var message = Message.Create(
            content,
            contentSerializer: contentSerializer,
            queue: queueName,
            headers: headers,
            partitionKey: partitionKey);
        queue.Enqueue(message);
    }

    /// <summary>
    /// Sends a strongly-typed content object using a specific content serializer.
    /// </summary>
    public static void Send<T>(
        this IQueue queue,
        T content,
        IContentSerializer contentSerializer,
        string? destinationUri = null,
        string? queueName = null,
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
        queue.Send(message);
    }
}
