using LightningDB;
using Bielu.PersistentQueues.Serialization;
using Bielu.PersistentQueues.Storage;
using Bielu.PersistentQueues.Storage.LMDB;
using Bielu.PersistentQueues.Tests.Storage.Shared;
using Shouldly;
using Xunit;
using Xunit.Abstractions;

namespace Bielu.PersistentQueues.Tests.Storage.Lmdb;

/// <summary>
/// Runs the shared IncomingMessageTests against the LMDB provider,
/// plus LMDB-specific incoming message tests.
/// </summary>
public class LmdbIncomingMessageTests(ITestOutputHelper output) : Shared.IncomingMessageTests(output)
{

    protected override IMessageStore CreateStoreForPath(string path)
    {
        var env = new LightningEnvironment(path, new EnvironmentConfiguration
        {
            MaxDatabases = 20,
            MapSize = 1024 * 1024 * 100
        });
        return new LmdbMessageStore(env, new MessageSerializer());
    }

    [Fact]
    public void crash_before_commit()
    {
        var path = TempPath();
        var message = NewMessage();
        using (var store = (LmdbMessageStore)CreateStoreForPath(path))
        {
            store.CreateQueue("test");
            using (var transaction = store.BeginTransaction())
            {
                store.StoreIncoming(transaction, message);
                //crash
            }
        }

        using var env2 = new LightningEnvironment(path, new EnvironmentConfiguration
        {
            MaxDatabases = 20,
            MapSize = 1024 * 1024 * 100
        });
        using var store2 = new LmdbMessageStore(env2, new MessageSerializer());
        store2.CreateQueue("test");
        var msg = store2.GetMessage("test", message.Id);
        // After crash (dispose without commit), message should not be persisted
        msg.ShouldBeNull();
    }

    [Fact]
    public void creating_multiple_stores()
    {
        var path1 = TempPath();
        var env1 = new LightningEnvironment(path1, new EnvironmentConfiguration { MaxDatabases = 20, MapSize = 1024 * 1024 * 100 });
        var store1 = new LmdbMessageStore(env1, new MessageSerializer());
        store1.Dispose();

        var path2 = TempPath();
        var env2 = new LightningEnvironment(path2, new EnvironmentConfiguration { MaxDatabases = 20, MapSize = 1024 * 1024 * 100 });
        var store2 = new LmdbMessageStore(env2, new MessageSerializer());
        store2.Dispose();

        var path3 = TempPath();
        var env3 = new LightningEnvironment(path3, new EnvironmentConfiguration { MaxDatabases = 20, MapSize = 1024 * 1024 * 100 });
        using var store3 = new LmdbMessageStore(env3, new MessageSerializer());
    }
}
