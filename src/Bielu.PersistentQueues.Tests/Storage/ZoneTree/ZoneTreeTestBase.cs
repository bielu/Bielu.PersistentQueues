using System;
using System.IO;
using System.Text;
using Bielu.PersistentQueues.Serialization;
using Bielu.PersistentQueues.Storage.ZoneTree;
using Xunit.Abstractions;

namespace Bielu.PersistentQueues.Tests.Storage.ZoneTree;

public class ZoneTreeTestBase
{
    private static readonly string TempBasePath = Path.Combine(
        Path.GetTempPath(), $"zonetreequeuestests-{Environment.Version}");

    protected ITestOutputHelper? Output { get; set; }

    protected static string TempPath()
    {
        var path = Path.Combine(TempBasePath, Guid.NewGuid().ToString());
        Directory.CreateDirectory(path);
        return path;
    }

    protected void ZoneTreeStorageScenario(Action<ZoneTreeMessageStore> action)
    {
        var path = TempPath();
        using var store = new ZoneTreeMessageStore(path, new MessageSerializer());
        store.CreateQueue("test");
        action(store);
    }

    protected static Message NewMessage(string queueName = "test", string payload = "hello")
    {
        return Message.Create(
            data: Encoding.UTF8.GetBytes(payload),
            queue: queueName
        );
    }
}
