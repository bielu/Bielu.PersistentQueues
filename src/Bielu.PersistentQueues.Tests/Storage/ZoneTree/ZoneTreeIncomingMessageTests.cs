using Bielu.PersistentQueues.Serialization;
using Bielu.PersistentQueues.Storage;
using Bielu.PersistentQueues.Storage.ZoneTree;
using Bielu.PersistentQueues.Tests.Storage.Shared;
using Shouldly;
using Xunit;
using Xunit.Abstractions;

namespace Bielu.PersistentQueues.Tests.Storage.ZoneTree;

/// <summary>
/// Runs the shared IncomingMessageTests against the ZoneTree provider,
/// plus ZoneTree-specific tests.
/// </summary>
public class ZoneTreeIncomingMessageTests(ITestOutputHelper output) : Shared.IncomingMessageTests(output)
{

    protected override IMessageStore CreateStoreForPath(string path)
    {
        return new ZoneTreeMessageStore(path, new MessageSerializer());
    }

    [Fact]
    public void creating_multiple_stores()
    {
        var path1 = TempPath();
        var store1 = new ZoneTreeMessageStore(path1, new MessageSerializer());
        store1.Dispose();

        var path2 = TempPath();
        var store2 = new ZoneTreeMessageStore(path2, new MessageSerializer());
        store2.Dispose();

        var path3 = TempPath();
        using var store3 = new ZoneTreeMessageStore(path3, new MessageSerializer());
    }
}
