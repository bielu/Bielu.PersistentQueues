using System.Collections.Generic;
using System.Linq;
using Bielu.PersistentQueues.Serialization;
using Bielu.PersistentQueues.Storage;
using Bielu.PersistentQueues.Storage.ZoneTree;
using Shouldly;
using Xunit;
using Xunit.Abstractions;

namespace Bielu.PersistentQueues.Tests.Storage.ZoneTree;

public class ZoneTreeIncomingMessageTests : ZoneTreeTestBase
{
    public ZoneTreeIncomingMessageTests(ITestOutputHelper output)
    {
        Output = output;
    }

    [Fact]
    public void happy_path_success()
    {
        ZoneTreeStorageScenario(store =>
        {
            var headers = new Dictionary<string, string> { ["my_key"] = "my_value" };
            var message = Message.Create(
                data: "hello"u8.ToArray(),
                queue: "test",
                headers: headers
            );
            store.CreateQueue(message.QueueString!);
            store.StoreIncoming(message);
            var msg = store.GetMessage(message.QueueString!, message.Id);
            System.Text.Encoding.UTF8.GetString(msg!.Value.DataArray!).ShouldBe("hello");
            msg.Value.GetHeadersDictionary().First().Value.ShouldBe("my_value");
        });
    }

    [Fact]
    public void storing_message_for_queue_that_doesnt_exist()
    {
        ZoneTreeStorageScenario(store =>
        {
            var message = NewMessage("blah");
            Should.Throw<QueueDoesNotExistException>(() => { store.StoreIncoming(message); });
        });
    }

    [Fact]
    public void rollback_messages_received()
    {
        ZoneTreeStorageScenario(store =>
        {
            var message = NewMessage();
            store.CreateQueue(message.QueueString!);
            using (var transaction = store.BeginTransaction())
            {
                store.StoreIncoming(transaction, message);
                // Not calling Commit - should discard operations
            }

            var msg = store.GetMessage(message.QueueString!, message.Id);
            msg.ShouldBeNull();
        });
    }

    [Fact]
    public void creating_multiple_stores()
    {
        ZoneTreeStorageScenario(store =>
        {
            store.Dispose();
            var path2 = TempPath();
            var store2 = new ZoneTreeMessageStore(path2, new MessageSerializer());
            store2.Dispose();
            var path3 = TempPath();
            using var store3 = new ZoneTreeMessageStore(path3, new MessageSerializer());
        });
    }
}
