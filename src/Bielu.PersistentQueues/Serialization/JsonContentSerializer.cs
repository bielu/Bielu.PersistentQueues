using System;
using System.Text.Json;

namespace Bielu.PersistentQueues.Serialization;

/// <summary>
/// Default content serializer using System.Text.Json.
/// </summary>
public class JsonContentSerializer(JsonSerializerOptions? options = null) : IContentSerializer
{
    /// <summary>
    /// A shared default instance with default <see cref="JsonSerializerOptions"/>.
    /// </summary>
    public static readonly JsonContentSerializer Default = new();

    private readonly JsonSerializerOptions? _options;

    /// <summary>
    /// Initializes a new instance of <see cref="JsonContentSerializer"/>.
    /// </summary>
    /// <param name="options">Optional JSON serializer options. If null, default options are used.</param>

    /// <inheritdoc />
    public byte[] Serialize<T>(T content)
    {
        return JsonSerializer.SerializeToUtf8Bytes(content, _options);
    }

    /// <inheritdoc />
    public T? Deserialize<T>(ReadOnlySpan<byte> data)
    {
        return JsonSerializer.Deserialize<T>(data, _options);
    }
}
