using System;
using Bielu.PersistentQueues.Serialization;
using Microsoft.Extensions.DependencyInjection;

namespace Bielu.PersistentQueues.Storage.ZoneTree;

/// <summary>
/// Extension methods for configuring ZoneTree storage with <see cref="PersistentQueuesBuilder"/>.
/// </summary>
public static class ZoneTreeServiceCollectionExtensions
{
    /// <summary>
    /// Registers ZoneTree-based persistent queue storage using the specified file system path and optional configuration.
    /// </summary>
    /// <param name="builder">The persistent queues builder to configure.</param>
    /// <param name="path">File system path where ZoneTree will store its data files.</param>
    /// <param name="configure">Optional action to customize <see cref="ZoneTreeStorageOptions"/> before registration.</param>
    /// <returns>The builder for chaining.</returns>
    public static PersistentQueuesBuilder AddZoneTreeStorage(
        this PersistentQueuesBuilder builder,
        string path,
        Action<ZoneTreeStorageOptions>? configure = null)
    {
        var options = new ZoneTreeStorageOptions();
        configure?.Invoke(options);

        builder.UseStorage(sp =>
        {
            var serializer = sp.GetRequiredService<IMessageSerializer>();
            return new ZoneTreeMessageStore(path, serializer, options);
        });

        return builder;
    }

    /// <summary>
    /// Registers ZoneTree-based persistent queue storage using the provided file path and storage options.
    /// </summary>
    /// <param name="builder">The persistent queues builder to configure.</param>
    /// <param name="path">The filesystem path where ZoneTree will store message data.</param>
    /// <param name="storageOptions">Configuration options for ZoneTree storage.</param>
    /// <returns>The original <see cref="PersistentQueuesBuilder"/> instance for chaining.</returns>
    public static PersistentQueuesBuilder AddZoneTreeStorage(
        this PersistentQueuesBuilder builder,
        string path,
        ZoneTreeStorageOptions storageOptions)
    {
        builder.UseStorage(sp =>
        {
            var serializer = sp.GetRequiredService<IMessageSerializer>();
            return new ZoneTreeMessageStore(path, serializer, storageOptions);
        });

        return builder;
    }
}
