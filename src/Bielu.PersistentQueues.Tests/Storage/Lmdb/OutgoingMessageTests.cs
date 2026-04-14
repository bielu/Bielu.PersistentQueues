using System.Collections.Generic;
using System.Linq;
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
/// Runs the shared OutgoingMessageTests against the LMDB provider,
/// plus LMDB-specific outgoing tests (e.g. raw wire format).
/// </summary>
public class LmdbOutgoingMessageTests(ITestOutputHelper output) : Shared.OutgoingMessageTests(output)
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
    public void persisted_outgoing_raw_returns_wire_format_with_routing_info()
    {
        StorageScenario(store =>
        {
            var headers = new Dictionary<string, string> { ["key1"] = "value1" };
            var message1 = Message.Create(
                data: "hello"u8.ToArray(),
                queue: "queue1",
                destinationUri: "lq.tcp://host1:5050"
            );
            var message2 = Message.Create(
                data: "world"u8.ToArray(),
                queue: "queue2",
                destinationUri: "lq.tcp://host2:6060",
                headers: headers
            );
            using (var tx = store.BeginTransaction())
            {
                store.StoreOutgoing(tx, message1);
                store.StoreOutgoing(tx, message2);
                tx.Commit();
            }

            var rawMessages = store.PersistedOutgoingRaw().ToList();

            rawMessages.Count.ShouldBe(2);

            // Verify first message
            var raw1 = rawMessages[0];
            WireFormatReader.GetQueueName(in raw1).ShouldBe("queue1");
            WireFormatReader.GetDestinationUri(in raw1).ShouldBe("lq.tcp://host1:5050");
            raw1.FullMessage.Length.ShouldBeGreaterThan(0);

            // Verify MessageId matches
            System.Span<byte> expectedId1 = stackalloc byte[16];
            message1.Id.MessageIdentifier.TryWriteBytes(expectedId1);
            raw1.MessageId.Span.SequenceEqual(expectedId1).ShouldBeTrue();

            // Verify second message
            var raw2 = rawMessages[1];
            WireFormatReader.GetQueueName(in raw2).ShouldBe("queue2");
            WireFormatReader.GetDestinationUri(in raw2).ShouldBe("lq.tcp://host2:6060");
            raw2.FullMessage.Length.ShouldBeGreaterThan(0);

            // Verify MessageId matches
            System.Span<byte> expectedId2 = stackalloc byte[16];
            message2.Id.MessageIdentifier.TryWriteBytes(expectedId2);
            raw2.MessageId.Span.SequenceEqual(expectedId2).ShouldBeTrue();
        });
    }
}
