using System.Linq;
using Shouldly;
using Xunit;
using Xunit.Abstractions;

namespace Bielu.PersistentQueues.Tests.Storage.Shared;

/// <summary>
/// Universal message store tests that run against every IMessageStore provider.
/// Derive from this class and implement <see cref="MessageStoreTestBase.CreateStoreForPath"/> for each provider.
/// </summary>
public abstract class MessageStoreTests(ITestOutputHelper? output = null) : MessageStoreTestBase(output)
{
    [Fact]
    public void getting_all_queues()
    {
        StorageScenario(store =>
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
        StorageScenario(store =>
        {
            store.ClearAllStorage();
        });
    }

    [Fact]
    public void clear_all_history_with_persistent_data()
    {
        StorageScenario(store =>
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
    public void retrieve_message_by_id()
    {
        StorageScenario(store =>
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
        StorageScenario(store =>
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
        StorageScenario(store =>
        {
            store.CreateQueue("to-delete");
            var msg = NewMessage("to-delete");
            store.StoreIncoming(msg);
            store.GetAllQueues().ShouldContain("to-delete");

            store.DeleteQueue("to-delete");
            store.GetAllQueues().ShouldNotContain("to-delete");
        });
    }

    [Fact]
    public void store_can_read_previously_stored_items()
    {
        var path = TempPath();
        using (var store = CreateStoreForPath(path))
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

        using var store2 = CreateStoreForPath(path);
        store2.CreateQueue("test");
        store2.PersistedIncoming("test").Count().ShouldBe(1);
        store2.PersistedOutgoing().Count().ShouldBe(1);
    }
}
