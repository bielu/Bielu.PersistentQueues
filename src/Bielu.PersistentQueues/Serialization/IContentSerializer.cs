using System;

namespace Bielu.PersistentQueues.Serialization;

/// <summary>
/// Defines the contract for serializing and deserializing message content (the Data payload).
/// </summary>
/// <remarks>
/// Implementations can use different serialization formats such as JSON, MessagePack, Protobuf, etc.
/// The default implementation uses System.Text.Json.
/// </remarks>
public interface IContentSerializer
{
    /// <summary>
    /// Serializes a strongly-typed object to a byte array for use as message data.
    /// </summary>
    /// <typeparam name="T">The type of the content to serialize.</typeparam>
    /// <param name="content">The content to serialize.</param>
    /// <returns>A byte array containing the serialized content.</returns>
    byte[] Serialize<T>(T content);

    /// <summary>
    /// Deserializes message data back to a strongly-typed object.
    /// </summary>
    /// <typeparam name="T">The type to deserialize the data to.</typeparam>
    /// <param name="data">The raw binary data to deserialize.</param>
    /// <returns>The deserialized object, or default if the data represents a null value.</returns>
    /// <exception cref="System.Exception">Thrown when deserialization fails (e.g., invalid format or type mismatch).</exception>
    T? Deserialize<T>(ReadOnlySpan<byte> data);
}
