using System;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Shouldly;
using Bielu.PersistentQueues.Storage.LMDB;
using Xunit;
using Xunit.Abstractions;

namespace Bielu.PersistentQueues.Tests;

public class DeadLetterQueueTests : TestBase
{
    public DeadLetterQueueTests(ITestOutputHelper output)
    {
        Output = output;
    }

    // ─── DeadLetterConstants ───────────────────────────────────────────────

    [Fact]
    public void GetDeadLetterQueueName_AppendsSuffix()
    {
        DeadLetterConstants.GetDeadLetterQueueName("orders").ShouldBe("orders:dead-letter");
    }

    [Fact]
    public void IsDeadLetterQueue_ReturnsTrueForDlq()
    {
        DeadLetterConstants.IsDeadLetterQueue("orders:dead-letter").ShouldBeTrue();
    }

    [Fact]
    public void IsDeadLetterQueue_ReturnsFalseForRegularQueue()
    {
        DeadLetterConstants.IsDeadLetterQueue("orders").ShouldBeFalse();
    }

    // ─── ProcessingAttempts tracking ──────────────────────────────────────

    [Fact]
    public void ProcessingAttempts_DefaultsToZero()
    {
        var message = NewMessage("test");
        message.ProcessingAttempts.ShouldBe(0);
    }

    [Fact]
    public void WithProcessingAttempts_UpdatesCounter()
    {
        var message = NewMessage("test").WithProcessingAttempts(3);
        message.ProcessingAttempts.ShouldBe(3);
    }

    [Fact]
    public void WithProcessingAttempts_DoesNotMutateOtherFields()
    {
        var original = NewMessage("test");
        var updated = original.WithProcessingAttempts(5);
        updated.QueueString.ShouldBe(original.QueueString);
        updated.Id.ShouldBe(original.Id);
        updated.Data.ToArray().ShouldBe(original.Data.ToArray());
    }

    // ─── MoveToDeadLetter – explicit via IQueueContext ─────────────────────

    [Fact]
    public async Task MoveToDeadLetter_MovesMessageToDlq()
    {
        await QueueScenario(async (queue, token) =>
        {
            queue.Enqueue(NewMessage("test"));

            var ctx = await queue.Receive("test", cancellationToken: token).FirstAsync(token);
            ctx.QueueContext.MoveToDeadLetter();
            ctx.QueueContext.CommitChanges();

            var dlqName = DeadLetterConstants.GetDeadLetterQueueName("test");
            var store = (LmdbMessageStore)queue.Store;
            store.PersistedIncoming("test").ShouldBeEmpty();
            store.PersistedIncoming(dlqName).Count().ShouldBe(1);
        }, TimeSpan.FromSeconds(3));
    }

    [Fact]
    public async Task MoveToDeadLetter_DlqQueueCreatedAutomatically()
    {
        await QueueScenario(async (queue, token) =>
        {
            queue.Enqueue(NewMessage("test"));

            var ctx = await queue.Receive("test", cancellationToken: token).FirstAsync(token);
            ctx.QueueContext.MoveToDeadLetter();
            ctx.QueueContext.CommitChanges();

            var dlqName = DeadLetterConstants.GetDeadLetterQueueName("test");
            queue.Queues.ShouldContain(dlqName);
        }, TimeSpan.FromSeconds(3));
    }

    [Fact]
    public async Task MoveToDeadLetter_CannotBeCalledAfterSuccessfullyReceived()
    {
        await QueueScenario(async (queue, token) =>
        {
            queue.Enqueue(NewMessage("test"));

            var ctx = await queue.Receive("test", cancellationToken: token).FirstAsync(token);
            ctx.QueueContext.SuccessfullyReceived();

            Should.Throw<InvalidOperationException>(() => ctx.QueueContext.MoveToDeadLetter());
        }, TimeSpan.FromSeconds(3));
    }

    // ─── Auto-DLQ via ReceiveLater exhausting MaxAttempts ─────────────────

    [Fact]
    public async Task ReceiveLater_WhenMaxAttemptsReached_MovesToDlq()
    {
        await QueueScenario(async (queue, token) =>
        {
            // maxAttempts=1 — first ReceiveLater should trigger auto-DLQ
            var message = Message.Create(
                data: Encoding.UTF8.GetBytes("hello"),
                queue: "test",
                maxAttempts: 1);
            queue.Enqueue(message);

            var ctx = await queue.Receive("test", cancellationToken: token).FirstAsync(token);
            ctx.QueueContext.ReceiveLater(TimeSpan.FromHours(1));
            ctx.QueueContext.CommitChanges();

            var dlqName = DeadLetterConstants.GetDeadLetterQueueName("test");
            var store = (LmdbMessageStore)queue.Store;
            store.PersistedIncoming("test").ShouldBeEmpty();
            store.PersistedIncoming(dlqName).Count().ShouldBe(1);
        }, TimeSpan.FromSeconds(3));
    }

    [Fact]
    public async Task ReceiveLater_WhenBelowMaxAttempts_DoesNotMoveToDlq()
    {
        await QueueScenario(async (queue, token) =>
        {
            var message = Message.Create(
                data: Encoding.UTF8.GetBytes("hello"),
                queue: "test",
                maxAttempts: 5);
            queue.Enqueue(message);

            var ctx = await queue.Receive("test", cancellationToken: token).FirstAsync(token);
            ctx.QueueContext.ReceiveLater(TimeSpan.FromMilliseconds(1));
            ctx.QueueContext.CommitChanges();

            var dlqName = DeadLetterConstants.GetDeadLetterQueueName("test");
            var store = (LmdbMessageStore)queue.Store;
            store.PersistedIncoming(dlqName).ShouldBeEmpty();
        }, TimeSpan.FromSeconds(3));
    }

    [Fact]
    public async Task ReceiveLater_WhenMaxAttemptsReached_DlqMessageHasIncrementedAttempts()
    {
        await QueueScenario(async (queue, token) =>
        {
            var message = Message.Create(
                data: Encoding.UTF8.GetBytes("hello"),
                queue: "test",
                maxAttempts: 1);
            queue.Enqueue(message);

            var ctx = await queue.Receive("test", cancellationToken: token).FirstAsync(token);
            ctx.QueueContext.ReceiveLater(TimeSpan.FromHours(1));
            ctx.QueueContext.CommitChanges();

            var dlqName = DeadLetterConstants.GetDeadLetterQueueName("test");
            var store = (LmdbMessageStore)queue.Store;
            var dlqMessage = store.PersistedIncoming(dlqName).Single();
            dlqMessage.ProcessingAttempts.ShouldBe(1);
        }, TimeSpan.FromSeconds(3));
    }

    // ─── MoveToDeadLetter – batch via IBatchQueueContext ──────────────────

    [Fact]
    public async Task Batch_MoveToDeadLetter_MovesAllMessagesToDlq()
    {
        await QueueScenario(async (queue, token) =>
        {
            queue.Enqueue(NewMessage("test", "msg1"));
            queue.Enqueue(NewMessage("test", "msg2"));

            var ctx = await queue.ReceiveBatch("test", maxMessages: 2, cancellationToken: token).FirstAsync(token);
            ctx.MoveToDeadLetter();
            ctx.CommitChanges();

            var dlqName = DeadLetterConstants.GetDeadLetterQueueName("test");
            var store = (LmdbMessageStore)queue.Store;
            store.PersistedIncoming("test").ShouldBeEmpty();
            store.PersistedIncoming(dlqName).Count().ShouldBe(2);
        }, TimeSpan.FromSeconds(3));
    }

    [Fact]
    public async Task Batch_MoveToDeadLetter_SubsetByMessage()
    {
        await QueueScenario(async (queue, token) =>
        {
            queue.Enqueue(NewMessage("test", "msg1"));
            queue.Enqueue(NewMessage("test", "msg2"));

            var ctx = await queue.ReceiveBatch("test", maxMessages: 2, cancellationToken: token).FirstAsync(token);
            var toDeadLetter = ctx.Messages.Take(1).ToArray();
            var toSuccess = ctx.Messages.Skip(1).ToArray();

            ctx.MoveToDeadLetter(toDeadLetter);
            ctx.SuccessfullyReceived(toSuccess);
            ctx.CommitChanges();

            var dlqName = DeadLetterConstants.GetDeadLetterQueueName("test");
            var store = (LmdbMessageStore)queue.Store;
            store.PersistedIncoming("test").ShouldBeEmpty();
            store.PersistedIncoming(dlqName).Count().ShouldBe(1);
        }, TimeSpan.FromSeconds(3));
    }

    [Fact]
    public async Task Batch_ReceiveLater_WhenMaxAttemptsReached_AutoMovesToDlq()
    {
        await QueueScenario(async (queue, token) =>
        {
            var m1 = Message.Create(data: "a"u8.ToArray(), queue: "test", maxAttempts: 1);
            var m2 = Message.Create(data: "b"u8.ToArray(), queue: "test", maxAttempts: 5);
            queue.Enqueue(m1);
            queue.Enqueue(m2);

            var ctx = await queue.ReceiveBatch("test", maxMessages: 2, cancellationToken: token).FirstAsync(token);
            ctx.ReceiveLater(TimeSpan.FromHours(1));
            ctx.CommitChanges();

            var dlqName = DeadLetterConstants.GetDeadLetterQueueName("test");
            var store = (LmdbMessageStore)queue.Store;
            // m1 (maxAttempts=1) should be dead-lettered; m2 (maxAttempts=5) should be retried
            store.PersistedIncoming(dlqName).Count().ShouldBe(1);
        }, TimeSpan.FromSeconds(3));
    }

    // ─── WithQueue helper ─────────────────────────────────────────────────

    [Fact]
    public void WithQueue_ChangesQueueName()
    {
        var message = NewMessage("test");
        var moved = message.WithQueue("other-queue");
        moved.QueueString.ShouldBe("other-queue");
        moved.DestinationUri.IsEmpty.ShouldBeTrue();
        moved.Id.ShouldBe(message.Id);
    }
}
