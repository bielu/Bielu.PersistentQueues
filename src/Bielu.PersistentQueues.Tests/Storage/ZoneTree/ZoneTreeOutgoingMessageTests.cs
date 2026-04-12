using Bielu.PersistentQueues.Serialization;
using Bielu.PersistentQueues.Storage;
using Bielu.PersistentQueues.Storage.ZoneTree;
using Bielu.PersistentQueues.Tests.Storage.Shared;
using Xunit.Abstractions;

namespace Bielu.PersistentQueues.Tests.Storage.ZoneTree;

/// <summary>
/// Runs the shared OutgoingMessageTests against the ZoneTree provider.
/// </summary>
public class ZoneTreeOutgoingMessageTests : Shared.OutgoingMessageTests
{
    public ZoneTreeOutgoingMessageTests(ITestOutputHelper output)
    {
        Output = output;
    }

    protected override IMessageStore CreateStoreForPath(string path)
    {
        return new ZoneTreeMessageStore(path, new MessageSerializer());
    }
}
