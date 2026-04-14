using Bielu.PersistentQueues.Serialization;
using Bielu.PersistentQueues.Storage;
using Bielu.PersistentQueues.Storage.ZoneTree;
using Bielu.PersistentQueues.Tests.Storage.Shared;
using Xunit.Abstractions;

namespace Bielu.PersistentQueues.Tests.Storage.ZoneTree;

/// <summary>
/// Runs the shared OutgoingMessageTests against the ZoneTree provider.
/// </summary>
public class ZoneTreeOutgoingMessageTests(ITestOutputHelper output) : Shared.OutgoingMessageTests
{

    protected override IMessageStore CreateStoreForPath(string path)
    {
        return new ZoneTreeMessageStore(path, new MessageSerializer());
    }
}
