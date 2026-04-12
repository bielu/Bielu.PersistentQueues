using System;
using System.IO;
using System.Text;
using Bielu.PersistentQueues.Storage;
using Xunit.Abstractions;

namespace Bielu.PersistentQueues.Tests.Storage.Shared;

/// <summary>
/// Abstract base for IMessageStore tests. Concrete derived classes provide
/// the store instance via <see cref="CreateStore"/> and <see cref="CreateStoreForPath"/>.
/// </summary>
public abstract class MessageStoreTestBase
{
    private static readonly string TempBasePath = Path.Combine(
        Path.GetTempPath(), $"pqtests-{Environment.Version}");

    protected ITestOutputHelper? Output { get; set; }

    /// <summary>
    /// Creates a new IMessageStore rooted at the given path.
    /// </summary>
    protected abstract IMessageStore CreateStoreForPath(string path);

    /// <summary>
    /// Creates a new IMessageStore at a fresh temp directory.
    /// </summary>
    protected IMessageStore CreateStore()
    {
        var path = TempPath();
        return CreateStoreForPath(path);
    }

    protected void StorageScenario(Action<IMessageStore> action)
    {
        using var store = CreateStore();
        store.CreateQueue("test");
        action(store);
    }

    protected static string TempPath()
    {
        var path = Path.Combine(TempBasePath, Guid.NewGuid().ToString());
        Directory.CreateDirectory(path);
        return path;
    }

    protected static Message NewMessage(string queueName = "test", string payload = "hello")
    {
        return Message.Create(
            data: Encoding.UTF8.GetBytes(payload),
            queue: queueName
        );
    }
}
