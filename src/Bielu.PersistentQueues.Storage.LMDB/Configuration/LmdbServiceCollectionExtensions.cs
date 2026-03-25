using System;
using Bielu.PersistentQueues.Serialization;
using LightningDB;
using Microsoft.Extensions.DependencyInjection;

namespace Bielu.PersistentQueues.Storage.LMDB;

/// <summary>
/// Extension methods for configuring LMDB storage with <see cref="PersistentQueuesBuilder"/>.
/// </summary>
public static class LmdbServiceCollectionExtensions
{
    /// <summary>
    /// Configures LMDB storage at the specified path.
    /// </summary>
    /// <param name="builder">The persistent queues builder.</param>
    /// <param name="path">The file system path for the LMDB environment.</param>
    /// <param name="configure">Optional delegate to configure LMDB storage options.</param>
    /// <returns>The builder for chaining.</returns>
    public static PersistentQueuesBuilder AddLmdbStorage(
        this PersistentQueuesBuilder builder,
        string path,
        Action<LmdbStorageConfiguration>? configure = null)
    {
        var config = new LmdbStorageConfiguration();
        configure?.Invoke(config);

        var envConfig = config.EnvironmentConfiguration;
        var storageOptions = config.StorageOptions;

        builder.UseStorage(sp =>
        {
            var serializer = sp.GetRequiredService<IMessageSerializer>();
            var env = new LightningEnvironment(path, envConfig);
            return new LmdbMessageStore(env, serializer, storageOptions);
        });

        return builder;
    }

    /// <summary>
    /// Configures LMDB storage with a custom environment factory.
    /// </summary>
    /// <param name="builder">The persistent queues builder.</param>
    /// <param name="environmentFactory">A factory function that creates the LightningEnvironment.</param>
    /// <param name="storageOptions">Optional LMDB storage options.</param>
    /// <returns>The builder for chaining.</returns>
    public static PersistentQueuesBuilder AddLmdbStorage(
        this PersistentQueuesBuilder builder,
        Func<LightningEnvironment> environmentFactory,
        LmdbStorageOptions? storageOptions = null)
    {
        builder.UseStorage(sp =>
        {
            var serializer = sp.GetRequiredService<IMessageSerializer>();
            return new LmdbMessageStore(environmentFactory(), serializer, storageOptions);
        });

        return builder;
    }
}

/// <summary>
/// Configuration options for LMDB storage setup.
/// </summary>
public class LmdbStorageConfiguration
{
    /// <summary>
    /// Gets or sets the LMDB environment configuration.
    /// </summary>
    public EnvironmentConfiguration EnvironmentConfiguration { get; set; } = new()
    {
        MaxDatabases = 5,
        MapSize = 1024 * 1024 * 100
    };

    /// <summary>
    /// Gets or sets the LMDB storage options.
    /// </summary>
    public LmdbStorageOptions? StorageOptions { get; set; }
}
