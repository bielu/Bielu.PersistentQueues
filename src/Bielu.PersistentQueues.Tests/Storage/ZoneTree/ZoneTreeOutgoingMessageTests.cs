using System;
using System.Collections.Generic;
using System.Linq;
using Bielu.PersistentQueues.Serialization;
using Shouldly;
using Xunit;
using Xunit.Abstractions;

namespace Bielu.PersistentQueues.Tests.Storage.ZoneTree;

public class ZoneTreeOutgoingMessageTests : ZoneTreeTestBase
{
    public ZoneTreeOutgoingMessageTests(ITestOutputHelper output)
    {
        Output = output;
    }

    [Fact]
    public void happy_path_messages_sent()
    {
        ZoneTreeStorageScenario(store =>
        {
            var message = Message.Create(
                data: "hello"u8.ToArray(),
                queue: "test",
                destinationUri: "lq.tcp://localhost:5050"
            );
            var headers = new Dictionary<string, string> { ["header"] = "header_value" };
            var message2 = Message.Create(
                data: "hello"u8.ToArray(),
                queue: "test",
                destinationUri: "lq.tcp://localhost:5050",
                deliverBy: DateTime.Now.AddSeconds(5),
                maxAttempts: 3,
                headers: headers
            );
            using (var tx = store.BeginTransaction())
            {
                store.StoreOutgoing(tx, message);
                store.StoreOutgoing(tx, message2);
                tx.Commit();
            }

            store.SuccessfullySent(message);

            var result = store.PersistedOutgoing().First();
            result.Id.ShouldBe(message2.Id);
            result.QueueString.ShouldBe(message2.QueueString);
            result.DataArray.ShouldBe(message2.DataArray);
            result.SentAt.ShouldBe(message2.SentAt);
            result.DeliverBy.ShouldBe(message2.DeliverBy);
            result.MaxAttempts.ShouldBe(message2.MaxAttempts);
            result.GetHeadersDictionary()["header"].ShouldBe("header_value");
        });
    }

    [Fact]
    public void failed_to_send_with_max_attempts()
    {
        ZoneTreeStorageScenario(store =>
        {
            var message = Message.Create(
                data: "hello"u8.ToArray(),
                queue: "test",
                destinationUri: "lq.tcp://localhost:5050",
                maxAttempts: 1
            ).WithSentAttempts(1);
            using (var tx = store.BeginTransaction())
            {
                store.StoreOutgoing(tx, message);
                tx.Commit();
            }

            store.FailedToSend(false, message);

            var outgoing = store.GetMessage("outgoing", message.Id);
            outgoing.ShouldBeNull();
        });
    }

    [Fact]
    public void failed_to_send_with_deliver_by()
    {
        ZoneTreeStorageScenario(store =>
        {
            var message = Message.Create(
                data: "hello"u8.ToArray(),
                queue: "test",
                destinationUri: "lq.tcp://localhost:5050",
                deliverBy: DateTime.Now
            );
            using (var tx = store.BeginTransaction())
            {
                store.StoreOutgoing(tx, message);
                tx.Commit();
            }

            store.FailedToSend(false, message);

            var outgoing = store.GetMessage("outgoing", message.Id);
            outgoing.ShouldBeNull();
        });
    }
}
