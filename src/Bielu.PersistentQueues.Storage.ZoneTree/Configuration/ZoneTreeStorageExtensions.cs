using System;

namespace Bielu.PersistentQueues.Storage.ZoneTree;

/// <summary>
/// Extension methods for configuring ZoneTree storage on <see cref="QueueConfiguration"/>.
/// </summary>
public static class ZoneTreeStorageExtensions
{
    /// <summary>
    /// Configures ZoneTree storage at the specified path with default options.
    /// </summary>
    /// <param name="configuration">The queue configuration.</param>
    /// <param name="path">The file system path for the ZoneTree data directory.</param>
    /// <returns>The configuration for chaining.</returns>
    public static QueueConfiguration StoreWithZoneTree(this QueueConfiguration configuration, string path)
    {
        return configuration.StoreWithZoneTree(path, null);
    }

    /// <summary>
    /// Configures ZoneTree storage at the specified path with custom options.
    /// </summary>
    /// <param name="configuration">The queue configuration.</param>
    /// <param name="path">The file system path for the ZoneTree data directory.</param>
    /// <param name="storageOptions">Optional ZoneTree storage options.</param>
    /// <returns>The configuration for chaining.</returns>
    public static QueueConfiguration StoreWithZoneTree(this QueueConfiguration configuration,
        string path, ZoneTreeStorageOptions? storageOptions)
    {
        return configuration.StoreMessagesWith(() => new ZoneTreeMessageStore(
            path,
            configuration.Serializer ?? throw new InvalidOperationException(
                "Serializer must be configured before storage. Call SerializeWith() first."),
            storageOptions));
    }
}
