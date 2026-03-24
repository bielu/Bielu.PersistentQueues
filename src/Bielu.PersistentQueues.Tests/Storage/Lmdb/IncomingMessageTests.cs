using System.Collections.Generic;
using System.Linq;
using Bielu.PersistentQueues.Serialization;
using Bielu.PersistentQueues.Storage;
using Bielu.PersistentQueues.Storage.LMDB;
using Shouldly;
using Xunit;
using Xunit.Abstractions;

namespace Bielu.PersistentQueues.Tests.Storage.Lmdb;

public class IncomingMessageTests : TestBase
{
    public IncomingMessageTests(ITestOutputHelper output)
    {
        Output = output;
    }

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
    public void crash_before_commit()
    {
        StorageScenario(store =>
        {
            var message = NewMessage();
            store.CreateQueue(message.QueueString!);
            using (var transaction = store.BeginTransaction())
            {
                store.StoreIncoming(transaction, message);
                //crash
            }

            store.Dispose();
            using var env = LightningEnvironment();
            using var store2 = new LmdbMessageStore(env, new MessageSerializer());
            store2.CreateQueue(message.QueueString!);
            var msg = store2.GetMessage(message.QueueString!, message.Id);
            msg.ShouldBeNull();
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
            }

            var msg = store.GetMessage(message.QueueString!, message.Id);
            msg.ShouldBeNull();
        });
    }

    [Fact]
    public void creating_multiple_stores()
    {
        StorageScenario(store =>
        {
            store.Dispose();
            using var env = LightningEnvironment();
            var store2 = new LmdbMessageStore(env, new MessageSerializer());
            store2.Dispose();
            using var env2 = LightningEnvironment();
            using var store3 = new LmdbMessageStore(env2, new MessageSerializer());
        });

    }
}