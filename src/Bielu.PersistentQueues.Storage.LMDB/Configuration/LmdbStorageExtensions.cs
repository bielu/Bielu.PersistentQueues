using System;
using LightningDB;

namespace Bielu.PersistentQueues.Storage.LMDB;

public static class LmdbStorageExtensions
{
    public static QueueConfiguration StoreWithLmdb(this QueueConfiguration configuration, string path,
        EnvironmentConfiguration config)
    {
        return configuration.StoreWithLmdb(() => new LightningEnvironment(path, config), null);
    }

    public static QueueConfiguration StoreWithLmdb(this QueueConfiguration configuration, string path,
        EnvironmentConfiguration config, LmdbStorageOptions? storageOptions)
    {
        return configuration.StoreWithLmdb(() => new LightningEnvironment(path, config), storageOptions);
    }

    /// <summary>
    /// Configures LMDB storage at the specified path with a <see cref="StorageSize"/> for map size.
    /// </summary>
    /// <param name="configuration">The queue configuration.</param>
    /// <param name="path">The file system path for the LMDB environment.</param>
    /// <param name="mapSize">The LMDB map size (e.g., <c>StorageSize.MB(100)</c> or <c>StorageSize.GB(2)</c>).</param>
    /// <param name="storageOptions">Optional LMDB storage options.</param>
    public static QueueConfiguration StoreWithLmdb(this QueueConfiguration configuration, string path,
        StorageSize mapSize, LmdbStorageOptions? storageOptions = null)
    {
        var config = new EnvironmentConfiguration
        {
            MaxDatabases = 5,
            MapSize = mapSize.Bytes
        };
        return configuration.StoreWithLmdb(() => new LightningEnvironment(path, config), storageOptions);
    }

    public static QueueConfiguration StoreWithLmdb(this QueueConfiguration configuration,
        Func<LightningEnvironment> environment)
    {
        return configuration.StoreWithLmdb(environment, null);
    }

    public static QueueConfiguration StoreWithLmdb(this QueueConfiguration configuration,
        Func<LightningEnvironment> environment, LmdbStorageOptions? storageOptions)
    {
        return configuration.StoreMessagesWith(() => new LmdbMessageStore(
            environment(),
            configuration.Serializer ?? throw new InvalidOperationException(
                "Serializer must be configured before storage. Call SerializeWith() first."),
            storageOptions));
    }
}