using System.Linq;
using Bielu.PersistentQueues.Serialization;
using Bielu.PersistentQueues.Storage.ZoneTree;
using Shouldly;
using Xunit;
using Xunit.Abstractions;

namespace Bielu.PersistentQueues.Tests.Storage.ZoneTree;

public class ZoneTreeMessageStoreTests : ZoneTreeTestBase
{
    public ZoneTreeMessageStoreTests(ITestOutputHelper output)
    {
        Output = output;
    }

    [Fact]
    public void getting_all_queues()
    {
        ZoneTreeStorageScenario(store =>
        {
            store.CreateQueue("test2");
            store.CreateQueue("test3");
            var queues = store.GetAllQueues();
            queues.ShouldContain("test");
            queues.ShouldContain("test2");
            queues.ShouldContain("test3");
            queues.Length.ShouldBe(3);
        });
    }

    [Fact]
    public void clear_all_history_with_empty_dataset_doesnt_throw()
    {
        ZoneTreeStorageScenario(store =>
        {
            store.ClearAllStorage();
        });
    }

    [Fact]
    public void clear_all_history_with_persistent_data()
    {
        ZoneTreeStorageScenario(store =>
        {
            var message = NewMessage("test");
            var outgoingMessage = Message.Create(
                data: "hello"u8.ToArray(),
                queue: "test",
                destinationUri: "lq.tcp://localhost:3030"
            );
            using (var tx = store.BeginTransaction())
            {
                store.StoreOutgoing(tx, outgoingMessage);
                store.StoreIncoming(tx, message);
                tx.Commit();
            }

            store.PersistedIncoming("test").Count().ShouldBe(1);
            store.PersistedOutgoing().Count().ShouldBe(1);
            store.ClearAllStorage();
            store.PersistedIncoming("test").Count().ShouldBe(0);
            store.PersistedOutgoing().Count().ShouldBe(0);
        });
    }

    [Fact]
    public void store_can_read_previously_stored_items()
    {
        var path = TempPath();
        using (var store = new ZoneTreeMessageStore(path, new MessageSerializer()))
        {
            store.CreateQueue("test");
            var message = NewMessage("test");
            var outgoingMessage = Message.Create(
                data: "hello"u8.ToArray(),
                queue: "test",
                destinationUri: "lq.tcp://localhost:3030"
            );
            using (var tx = store.BeginTransaction())
            {
                store.StoreOutgoing(tx, outgoingMessage);
                store.StoreIncoming(tx, message);
                tx.Commit();
            }
        }

        using var store2 = new ZoneTreeMessageStore(path, new MessageSerializer());
        store2.CreateQueue("test");
        store2.PersistedIncoming("test").Count().ShouldBe(1);
        store2.PersistedOutgoing().Count().ShouldBe(1);
    }

    [Fact]
    public void retrieve_message_by_id()
    {
        ZoneTreeStorageScenario(store =>
        {
            var message = NewMessage("test");
            var outgoingMessage = Message.Create(
                data: "hello"u8.ToArray(),
                queue: "test",
                destinationUri: "lq.tcp://localhost:3030"
            );
            using (var tx = store.BeginTransaction())
            {
                store.StoreOutgoing(tx, outgoingMessage);
                store.StoreIncoming(tx, message);
                tx.Commit();
            }

            var message2 = store.GetMessage(message.QueueString!, message.Id);
            var outgoing2 = store.GetMessage("outgoing", outgoingMessage.Id);
            message2.ShouldNotBeNull();
            outgoing2.ShouldNotBeNull();
        });
    }

    [Fact]
    public void get_message_count()
    {
        ZoneTreeStorageScenario(store =>
        {
            store.GetMessageCount("test").ShouldBe(0);

            var msg1 = NewMessage("test", "msg1");
            var msg2 = NewMessage("test", "msg2");
            store.StoreIncoming(msg1, msg2);

            store.GetMessageCount("test").ShouldBe(2);
        });
    }

    [Fact]
    public void delete_queue()
    {
        ZoneTreeStorageScenario(store =>
        {
            store.CreateQueue("to-delete");
            var msg = NewMessage("to-delete");
            store.StoreIncoming(msg);
            store.GetAllQueues().ShouldContain("to-delete");

            store.DeleteQueue("to-delete");
            store.GetAllQueues().ShouldNotContain("to-delete");
        });
    }
}
