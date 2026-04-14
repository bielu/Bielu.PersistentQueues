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
    /// <param name="path">The root directory path where the store will persist its data.</param>
    /// <returns>An IMessageStore instance rooted at <paramref name="path"/>.</returns>
    protected abstract IMessageStore CreateStoreForPath(string path);

    /// <summary>
    /// Creates a fresh temporary directory and returns an IMessageStore rooted at that directory.
    /// </summary>
    /// <returns>An IMessageStore instance whose storage root is a newly created temporary directory.</returns>
    protected IMessageStore CreateStore()
    {
        var path = TempPath();
        return CreateStoreForPath(path);
    }

    /// <summary>
    /// Creates a temporary IMessageStore with a "test" queue, invokes the provided action with that store, and disposes the store when the action completes.
    /// </summary>
    /// <param name="action">Action to execute using the initialized IMessageStore; the store will be disposed after the action returns.</param>
    protected void StorageScenario(Action<IMessageStore> action)
    {
        using var store = CreateStore();
        store.CreateQueue("test");
        action(store);
    }

    /// <summary>
    /// Creates a new unique subdirectory under the test temporary root and returns its path.
    /// </summary>
    /// <returns>The full path of the created temporary directory.</returns>
    protected static string TempPath()
    {
        var path = Path.Combine(TempBasePath, Guid.NewGuid().ToString());
        Directory.CreateDirectory(path);
        return path;
    }

    /// <summary>
    /// Creates a Message for the specified queue with the given payload encoded as UTF-8 bytes.
    /// </summary>
    /// <param name="queueName">Target queue name for the message.</param>
    /// <param name="payload">String payload to encode into the message body.</param>
    /// <returns>A Message whose data is the UTF-8 bytes of <paramref name="payload"/> and whose queue is <paramref name="queueName"/>.</returns>
    protected static Message NewMessage(string queueName = "test", string payload = "hello")
    {
        return Message.Create(
            data: Encoding.UTF8.GetBytes(payload),
            queue: queueName
        );
    }
}
