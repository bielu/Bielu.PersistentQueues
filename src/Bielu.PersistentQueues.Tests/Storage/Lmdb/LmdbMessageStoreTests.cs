using LightningDB;
using Bielu.PersistentQueues.Serialization;
using Bielu.PersistentQueues.Storage;
using Bielu.PersistentQueues.Storage.LMDB;
using Bielu.PersistentQueues.Tests.Storage.Shared;
using Xunit.Abstractions;

namespace Bielu.PersistentQueues.Tests.Storage.Lmdb;

/// <summary>
/// Runs the shared MessageStoreTests against the LMDB provider.
/// </summary>
public class LmdbMessageStoreTests : MessageStoreTests
{
    public LmdbMessageStoreTests(ITestOutputHelper output)
    {
        Output = output;
    }

    protected override IMessageStore CreateStoreForPath(string path)
    {
        var env = new LightningEnvironment(path, new EnvironmentConfiguration
        {
            MaxDatabases = 20,
            MapSize = 1024 * 1024 * 100
        });
        return new LmdbMessageStore(env, new MessageSerializer());
    }
}
