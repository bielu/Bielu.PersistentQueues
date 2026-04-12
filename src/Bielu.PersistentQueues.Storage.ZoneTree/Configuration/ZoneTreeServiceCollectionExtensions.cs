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
    /// Configures ZoneTree storage at the specified path.
    /// </summary>
    /// <param name="builder">The persistent queues builder.</param>
    /// <param name="path">The file system path for the ZoneTree data directory.</param>
    /// <param name="configure">Optional delegate to configure ZoneTree storage options.</param>
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
    /// Configures ZoneTree storage at the specified path with explicit options.
    /// </summary>
    /// <param name="builder">The persistent queues builder.</param>
    /// <param name="path">The file system path for the ZoneTree data directory.</param>
    /// <param name="storageOptions">ZoneTree storage options.</param>
    /// <returns>The builder for chaining.</returns>
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
