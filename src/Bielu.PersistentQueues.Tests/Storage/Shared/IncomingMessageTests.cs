using System.Collections.Generic;
using System.Linq;
using Bielu.PersistentQueues.Storage;
using Shouldly;
using Xunit;

namespace Bielu.PersistentQueues.Tests.Storage.Shared;

/// <summary>
/// Universal incoming message tests that run against every IMessageStore provider.
/// </summary>
public abstract class IncomingMessageTests : MessageStoreTestBase
{
    [Fact]
    public void happy_path_success()
    {
        StorageScenario(store =>
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
        StorageScenario(store =>
        {
            var message = NewMessage("blah");
            Should.Throw<QueueDoesNotExistException>(() => { store.StoreIncoming(message); });
        });
    }

    [Fact]
    public void rollback_messages_received()
    {
        StorageScenario(store =>
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
}
