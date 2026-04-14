using System;
using System.Threading.Tasks;
using System.Linq;
using System.Text;
using Shouldly;
using Bielu.PersistentQueues.Storage.LMDB;
using Xunit;
using Xunit.Abstractions;

namespace Bielu.PersistentQueues.Tests;

public class QueueContextTests(ITestOutputHelper output) : TestBase
{

    [Fact]
    public async Task SuccessfullyReceived_RemovesMessageFromQueue()
    {
        await QueueScenarioAsync(async (queue, token) =>
        {
            var message = NewMessage("test");
            queue.Enqueue(message);
            
            var receivedContext = await queue.Receive("test", cancellationToken: token).FirstAsync(token);
            
            receivedContext.QueueContext.SuccessfullyReceived();
            receivedContext.QueueContext.CommitChanges();
            
            var store = (LmdbMessageStore)queue.Store;
            store.PersistedIncoming("test").Any().ShouldBeFalse();
            
            var allMessages = store.PersistedIncoming("test").ToList();
            allMessages.Count.ShouldBe(0);
        }, TimeSpan.FromSeconds(3));
    }
    
    [Fact]
    public async Task MoveTo_MovesMessageToAnotherQueue()
    {
        await QueueScenarioAsync(async (queue, token) =>
        {
            queue.CreateQueue("another");
            var message = NewMessage("test");
            queue.Enqueue(message);
            
            var receivedContext = await queue.Receive("test", cancellationToken: token).FirstAsync(token);
            
            receivedContext.QueueContext.MoveTo("another");
            receivedContext.QueueContext.CommitChanges();
            
            var movedMessage = await queue.Receive("another", cancellationToken: token).FirstAsync(token);
            movedMessage.Message.QueueString.ShouldBe("another");
            
            var store = (LmdbMessageStore)queue.Store;
            store.PersistedIncoming("test").Any().ShouldBeFalse();
        });
    }
    
    [Fact]
    public async Task Send_EnqueuesOutgoingMessage()
    {
        await QueueScenarioAsync(async (queue, token) =>
        {
            queue.CreateQueue("response");
            
            var receivedMessage = NewMessage("test");
            queue.Enqueue(receivedMessage);
            
            var receivedContext = await queue.Receive("test", cancellationToken: token).FirstAsync(token);
            
            var responseMessage = Message.Create(
                data: "hello"u8.ToArray(),
                queue: "response",
                destinationUri: $"lq.tcp://localhost:{queue.Endpoint.Port}"
            );
            
            receivedContext.QueueContext.Send(responseMessage);
            receivedContext.QueueContext.CommitChanges();
            
            var sent = await queue.Receive("response", cancellationToken: token).FirstAsync(token);
            sent.Message.QueueString.ShouldBe("response");
        }, TimeSpan.FromSeconds(3));
    }
    
    [Fact]
    public async Task ReceiveLater_WithTimeSpan_DelaysProcessing()
    {
        await QueueScenarioAsync(async (queue, token) =>
        {
            var message = NewMessage("test");
            queue.Enqueue(message);
            
            var receivedContext = await queue.Receive("test", cancellationToken: token).FirstAsync(token);
            var messageId = receivedContext.Message.Id;
            
            receivedContext.QueueContext.ReceiveLater(TimeSpan.FromMilliseconds(800));
            // ReceiveLater now handles message removal automatically, no need to call SuccessfullyReceived
            receivedContext.QueueContext.CommitChanges();
            
            var store = (LmdbMessageStore)queue.Store;
            store.PersistedIncoming("test").Any().ShouldBeFalse();
            
            await DeterministicDelayAsync(1000, token);
            
            var delayedMessage = await queue.Receive("test", cancellationToken: token).FirstAsync(token);
            delayedMessage.Message.Id.ShouldBe(messageId);
        }, TimeSpan.FromSeconds(5));
    }
    
    [Fact]
    public async Task MessageId_RemainsConstant_AcrossMultipleReads()
    {
        await QueueScenarioAsync(async (queue, token) =>
        {
            // Create a message with a specific ID
            var originalMessage = Message.Create(
                data: Encoding.UTF8.GetBytes("test data"),
                queue: "test");
            var originalId = originalMessage.Id;

            queue.Enqueue(originalMessage);

            // First read
            var ctx1 = await queue.Receive("test", cancellationToken: token).FirstAsync(token);
            ctx1.Message.Id.ShouldBe(originalId);
            ctx1.Message.Id.SourceInstanceId.ShouldBe(originalId.SourceInstanceId);
            ctx1.Message.Id.MessageIdentifier.ShouldBe(originalId.MessageIdentifier);

            // Delay and re-read
            ctx1.QueueContext.ReceiveLater(TimeSpan.FromMilliseconds(100));
            ctx1.QueueContext.CommitChanges();

            await DeterministicDelayAsync(200, token);

            // Second read - should have exact same ID
            var ctx2 = await queue.Receive("test", cancellationToken: token).FirstAsync(token);
            ctx2.Message.Id.ShouldBe(originalId);
            ctx2.Message.Id.SourceInstanceId.ShouldBe(originalId.SourceInstanceId);
            ctx2.Message.Id.MessageIdentifier.ShouldBe(originalId.MessageIdentifier);

            // Delay again and re-read
            ctx2.QueueContext.ReceiveLater(TimeSpan.FromMilliseconds(100));
            ctx2.QueueContext.CommitChanges();

            await DeterministicDelayAsync(200, token);

            // Third read - should still have exact same ID
            var ctx3 = await queue.Receive("test", cancellationToken: token).FirstAsync(token);
            ctx3.Message.Id.ShouldBe(originalId);
            ctx3.Message.Id.SourceInstanceId.ShouldBe(originalId.SourceInstanceId);
            ctx3.Message.Id.MessageIdentifier.ShouldBe(originalId.MessageIdentifier);

            ctx3.QueueContext.SuccessfullyReceived();
            ctx3.QueueContext.CommitChanges();
        }, TimeSpan.FromSeconds(5));
    }

    [Fact]
    public async Task ReceiveLater_WithDateTimeOffset_DelaysProcessing()
    {
        await QueueScenarioAsync(async (queue, token) =>
        {
            var message = NewMessage("test");
            queue.Enqueue(message);

            var receivedContext = await queue.Receive("test", cancellationToken: token).FirstAsync(token);
            var messageId = receivedContext.Message.Id;

            var futureTime = DateTimeOffset.Now.AddMilliseconds(800);
            receivedContext.QueueContext.ReceiveLater(futureTime);
            // ReceiveLater now handles message removal automatically, no need to call SuccessfullyReceived
            receivedContext.QueueContext.CommitChanges();

            var store = (LmdbMessageStore)queue.Store;
            store.PersistedIncoming("test").Any().ShouldBeFalse();
            
            await DeterministicDelayAsync(1000, token);
            
            var delayedMessage = await queue.Receive("test", cancellationToken: token).FirstAsync(token);
            delayedMessage.Message.Id.ShouldBe(messageId);
        }, TimeSpan.FromSeconds(5));
    }
    
    [Fact]
    public async Task Enqueue_AddsMessageToQueue()
    {
        await QueueScenarioAsync(async (queue, token) =>
        {
            var receivedMessage = NewMessage("test");
            queue.Enqueue(receivedMessage);
            
            var receivedContext = await queue.Receive("test", cancellationToken: token).FirstAsync(token);
            
            var newMessage = NewMessage("test", "new payload");
            var newMessageId = newMessage.Id;
            
            receivedContext.QueueContext.Enqueue(newMessage);
            
            receivedContext.QueueContext.SuccessfullyReceived();
            receivedContext.QueueContext.CommitChanges();
            
            var enqueuedMessage = await queue.Receive("test", cancellationToken: token).FirstAsync(token);
            enqueuedMessage.Message.Id.ShouldBe(newMessageId);
            Encoding.UTF8.GetString(enqueuedMessage.Message.DataArray!).ShouldBe("new payload");
        }, TimeSpan.FromSeconds(3));
    }
    
    [Fact]
    public async Task CommitChanges_ExecutesAllPendingActions()
    {
        await QueueScenarioAsync(async (queue, token) =>
        {
            queue.CreateQueue("response");
            
            var message = NewMessage("test");
            queue.Enqueue(message);
            
            var receivedContext = await queue.Receive("test", cancellationToken: token).FirstAsync(token);
            
            var messageToMove = NewMessage("test", "move me");
            var messageToSend = Message.Create(
                data: "hello"u8.ToArray(),
                queue: "response",
                destinationUri: $"lq.tcp://localhost:{queue.Endpoint.Port}"
            );
            var messageToEnqueue = NewMessage("test", "enqueued");
            
            var originalMessageId = message.Id;
            var moveMessageId = messageToMove.Id;
            var sendMessageId = messageToSend.Id;
            var enqueueMessageId = messageToEnqueue.Id;
            
            receivedContext.QueueContext.SuccessfullyReceived(); 
            receivedContext.QueueContext.Enqueue(messageToMove); 
            receivedContext.QueueContext.Send(messageToSend);    
            receivedContext.QueueContext.Enqueue(messageToEnqueue); 
            
            receivedContext.QueueContext.CommitChanges();
            
            
            var store = (LmdbMessageStore)queue.Store;
            var originalStillExists = store.PersistedIncoming("test")
                .Any(m => m.Id == originalMessageId);
            originalStillExists.ShouldBeFalse();
            
            var enqueuedMessages = await queue.Receive("test", cancellationToken: token)
                .Take(2)
                .ToListAsync(token);
            
            enqueuedMessages.Count.ShouldBe(2);
            
            var foundIds = enqueuedMessages.Select(m => m.Message.Id).ToList();
            foundIds.ShouldContain(moveMessageId);
            foundIds.ShouldContain(enqueueMessageId);
            
            var sentMessage = await queue.Receive("response", cancellationToken: token).FirstAsync(token);
            sentMessage.Message.Id.ShouldBe(sendMessageId);
        }, TimeSpan.FromSeconds(5));
    }
    
    [Fact]
    public async Task BatchContext_RemovesAllMessagesFromQueue()
    {
        await QueueScenarioAsync(async (queue, token) =>
        {
            var msg1 = NewMessage("test", "msg1");
            var msg2 = NewMessage("test", "msg2");
            var msg3 = NewMessage("test", "msg3");
            queue.Enqueue(msg1);
            queue.Enqueue(msg2);
            queue.Enqueue(msg3);
            
            var batchCtx = await queue.ReceiveBatch("test", cancellationToken: token).FirstAsync(token);
            batchCtx.Messages.Length.ShouldBe(3);
            
            batchCtx.SuccessfullyReceived();
            batchCtx.CommitChanges();
            
            // Verify every single message was removed, not just the first
            var store = (LmdbMessageStore)queue.Store;
            var remaining = store.PersistedIncoming("test").ToList();
            remaining.Count.ShouldBe(0);
            store.PersistedIncoming("test").Any(m => m.Id == msg1.Id).ShouldBeFalse();
            store.PersistedIncoming("test").Any(m => m.Id == msg2.Id).ShouldBeFalse();
            store.PersistedIncoming("test").Any(m => m.Id == msg3.Id).ShouldBeFalse();
        }, TimeSpan.FromSeconds(5));
    }
    
    [Fact]
    public async Task BatchContext_MovesAllMessagesToAnotherQueue()
    {
        await QueueScenarioAsync(async (queue, token) =>
        {
            queue.CreateQueue("other");
            queue.Enqueue(NewMessage("test", "msg1"));
            queue.Enqueue(NewMessage("test", "msg2"));
            
            var batchCtx = await queue.ReceiveBatch("test", cancellationToken: token).FirstAsync(token);
            batchCtx.Messages.Length.ShouldBe(2);
            
            batchCtx.MoveTo("other");
            batchCtx.CommitChanges();
            
            var store = (LmdbMessageStore)queue.Store;
            store.PersistedIncoming("test").Any().ShouldBeFalse();
            
            var movedMessages = await queue.Receive("other", cancellationToken: token)
                .Take(2)
                .ToListAsync(token);
            movedMessages.Count.ShouldBe(2);
            movedMessages.All(m => m.Message.QueueString == "other").ShouldBeTrue();
        }, TimeSpan.FromSeconds(5));
    }
    
    [Fact]
    public async Task BatchContext_ExposesAllMessages()
    {
        await QueueScenarioAsync(async (queue, token) =>
        {
            queue.Enqueue(NewMessage("test", "msg1"));
            queue.Enqueue(NewMessage("test", "msg2"));
            queue.Enqueue(NewMessage("test", "msg3"));
            
            var batchCtx = await queue.ReceiveBatch("test", cancellationToken: token).FirstAsync(token);
            
            batchCtx.Messages.Length.ShouldBe(3);
            var payloads = batchCtx.Messages
                .Select(m => Encoding.UTF8.GetString(m.DataArray!))
                .ToList();
            payloads.ShouldContain("msg1");
            payloads.ShouldContain("msg2");
            payloads.ShouldContain("msg3");
        }, TimeSpan.FromSeconds(5));
    }
    
    [Fact]
    public async Task BatchContext_SingleMessageBatch()
    {
        await QueueScenarioAsync(async (queue, token) =>
        {
            queue.Enqueue(NewMessage("test", "msg1"));
            
            var batchCtx = await queue.ReceiveBatch("test", cancellationToken: token).FirstAsync(token);
            batchCtx.Messages.Length.ShouldBe(1);
            
            batchCtx.SuccessfullyReceived();
            batchCtx.CommitChanges();
            
            var store = (LmdbMessageStore)queue.Store;
            store.PersistedIncoming("test").Any().ShouldBeFalse();
        }, TimeSpan.FromSeconds(5));
    }
    
    [Fact]
    public async Task SingleReceive_StillUsesPerMessageContext()
    {
        await QueueScenarioAsync(async (queue, token) =>
        {
            var message = NewMessage("test");
            queue.Enqueue(message);
            
            // Single Receive should not be affected by BatchQueueContext changes
            var receivedContext = await queue.Receive("test", cancellationToken: token).FirstAsync(token);
            
            receivedContext.QueueContext.SuccessfullyReceived();
            receivedContext.QueueContext.CommitChanges();
            
            var store = (LmdbMessageStore)queue.Store;
            store.PersistedIncoming("test").Any().ShouldBeFalse();
        }, TimeSpan.FromSeconds(3));
    }
    
    [Fact]
    public async Task BatchContext_MovesSubsetOfMessagesToAnotherQueue()
    {
        await QueueScenarioAsync(async (queue, token) =>
        {
            queue.CreateQueue("moved");
            
            var msg1 = NewMessage("test", "msg1");
            var msg2 = NewMessage("test", "msg2");
            var msg3 = NewMessage("test", "msg3");
            var msg4 = NewMessage("test", "msg4");
            
            queue.Enqueue(msg1);
            queue.Enqueue(msg2);
            queue.Enqueue(msg3);
            queue.Enqueue(msg4);
            
            var batchCtx = await queue.ReceiveBatch("test", cancellationToken: token).FirstAsync(token);
            batchCtx.Messages.Length.ShouldBe(4);
            
            // Move only msg1 and msg3 to another queue, mark msg2 and msg4 as successfully received
            var messagesToMove = new[] { msg1.Id.MessageIdentifier, msg3.Id.MessageIdentifier };
            var messagesToConfirm = new[] { msg2.Id.MessageIdentifier, msg4.Id.MessageIdentifier };
            
            batchCtx.MoveTo("moved", batchCtx.Messages.Where(m => messagesToMove.Contains(m.Id.MessageIdentifier)).ToArray());
            batchCtx.SuccessfullyReceived(messagesToConfirm);
            batchCtx.CommitChanges();
            
            var store = (LmdbMessageStore)queue.Store;
            // Original queue should be empty
            store.PersistedIncoming("test").Any().ShouldBeFalse();
            
            // Verify moved messages are in the "moved" queue
            var movedMessages = await queue.Receive("moved", cancellationToken: token)
                .Take(2)
                .ToListAsync(token);
            movedMessages.Count.ShouldBe(2);
            movedMessages.All(m => m.Message.QueueString == "moved").ShouldBeTrue();
            var movedIds = movedMessages.Select(m => m.Message.Id.MessageIdentifier).ToList();
            movedIds.ShouldContain(msg1.Id.MessageIdentifier);
            movedIds.ShouldContain(msg3.Id.MessageIdentifier);
            
            // Verify confirmed messages are gone
            store.PersistedIncoming("test").Any(m => m.Id.MessageIdentifier == msg2.Id.MessageIdentifier).ShouldBeFalse();
            store.PersistedIncoming("test").Any(m => m.Id.MessageIdentifier == msg4.Id.MessageIdentifier).ShouldBeFalse();
        }, TimeSpan.FromSeconds(5));
    }
    
    [Fact]
    public async Task BatchContext_ReceiveLaterSubsetOfMessages()
    {
        await QueueScenarioAsync(async (queue, token) =>
        {
            var msg1 = NewMessage("test", "msg1");
            var msg2 = NewMessage("test", "msg2");
            var msg3 = NewMessage("test", "msg3");
            var msg4 = NewMessage("test", "msg4");
            
            queue.Enqueue(msg1);
            queue.Enqueue(msg2);
            queue.Enqueue(msg3);
            queue.Enqueue(msg4);
            
            var batchCtx = await queue.ReceiveBatch("test", cancellationToken: token).FirstAsync(token);
            batchCtx.Messages.Length.ShouldBe(4);
            
            // Delay msg1 and msg3, mark msg2 and msg4 as successfully received
            var messagesToDelay = new[] { msg1.Id.MessageIdentifier, msg3.Id.MessageIdentifier };
            var messagesToConfirm = new[] { msg2.Id.MessageIdentifier, msg4.Id.MessageIdentifier };
            
            batchCtx.ReceiveLater(messagesToDelay, TimeSpan.FromMilliseconds(800));
            batchCtx.SuccessfullyReceived(messagesToConfirm);
            batchCtx.CommitChanges();
            
            var store = (LmdbMessageStore)queue.Store;
            // Queue should be empty immediately after commit
            store.PersistedIncoming("test").Any().ShouldBeFalse();
            
            // Verify confirmed messages (msg2, msg4) are permanently gone
            store.PersistedIncoming("test").Any(m => m.Id.MessageIdentifier == msg2.Id.MessageIdentifier).ShouldBeFalse();
            store.PersistedIncoming("test").Any(m => m.Id.MessageIdentifier == msg4.Id.MessageIdentifier).ShouldBeFalse();
            
            // Wait for delayed messages to be re-enqueued
            await DeterministicDelayAsync(1000, token);
            
            // Verify delayed messages reappear
            var delayedMessages = await queue.Receive("test", cancellationToken: token)
                .Take(2)
                .ToListAsync(token);
            delayedMessages.Count.ShouldBe(2);
            var delayedIds = delayedMessages.Select(m => m.Message.Id.MessageIdentifier).ToList();
            delayedIds.ShouldContain(msg1.Id.MessageIdentifier);
            delayedIds.ShouldContain(msg3.Id.MessageIdentifier);
            
            // Verify confirmed messages are still gone after delayed messages reappear
            var allMessages = store.PersistedIncoming("test").ToList();
            allMessages.Count.ShouldBe(2); // Only msg1 and msg3 should be in storage
            allMessages.Any(m => m.Id.MessageIdentifier == msg2.Id.MessageIdentifier).ShouldBeFalse();
            allMessages.Any(m => m.Id.MessageIdentifier == msg4.Id.MessageIdentifier).ShouldBeFalse();
        }, TimeSpan.FromSeconds(5));
    }
    
    [Fact]
    public async Task BatchContext_MixMoveToAndReceiveLaterOnDifferentMessages()
    {
        await QueueScenarioAsync(async (queue, token) =>
        {
            queue.CreateQueue("moved");
            
            var msg1 = NewMessage("test", "msg1");
            var msg2 = NewMessage("test", "msg2");
            var msg3 = NewMessage("test", "msg3");
            var msg4 = NewMessage("test", "msg4");
            var msg5 = NewMessage("test", "msg5");
            
            queue.Enqueue(msg1);
            queue.Enqueue(msg2);
            queue.Enqueue(msg3);
            queue.Enqueue(msg4);
            queue.Enqueue(msg5);
            
            var batchCtx = await queue.ReceiveBatch("test", cancellationToken: token).FirstAsync(token);
            batchCtx.Messages.Length.ShouldBe(5);
            
            // Move msg1 and msg2 to another queue
            var messagesToMove = new[] { msg1.Id.MessageIdentifier, msg2.Id.MessageIdentifier };
            // Delay msg3 and msg4
            var messagesToDelay = new[] { msg3.Id.MessageIdentifier, msg4.Id.MessageIdentifier };
            // Confirm msg5
            var messagesToConfirm = new[] { msg5.Id.MessageIdentifier };
            
            batchCtx.MoveTo("moved", batchCtx.Messages.Where(m => messagesToMove.Contains(m.Id.MessageIdentifier)).ToArray());
            batchCtx.ReceiveLater(messagesToDelay, TimeSpan.FromMilliseconds(800));
            batchCtx.SuccessfullyReceived(messagesToConfirm);
            batchCtx.CommitChanges();
            
            var store = (LmdbMessageStore)queue.Store;
            // Original queue should be empty immediately
            store.PersistedIncoming("test").Any().ShouldBeFalse();
            
            // Verify moved messages are in the "moved" queue
            var movedMessages = await queue.Receive("moved", cancellationToken: token)
                .Take(2)
                .ToListAsync(token);
            movedMessages.Count.ShouldBe(2);
            movedMessages.All(m => m.Message.QueueString == "moved").ShouldBeTrue();
            var movedIds = movedMessages.Select(m => m.Message.Id.MessageIdentifier).ToList();
            movedIds.ShouldContain(msg1.Id.MessageIdentifier);
            movedIds.ShouldContain(msg2.Id.MessageIdentifier);
            
            // Verify confirmed message is gone
            store.PersistedIncoming("test").Any(m => m.Id.MessageIdentifier == msg5.Id.MessageIdentifier).ShouldBeFalse();
            
            // Wait for delayed messages to be re-enqueued
            await DeterministicDelayAsync(1000, token);
            
            // Verify delayed messages reappear in original queue
            var delayedMessages = await queue.Receive("test", cancellationToken: token)
                .Take(2)
                .ToListAsync(token);
            delayedMessages.Count.ShouldBe(2);
            var delayedIds = delayedMessages.Select(m => m.Message.Id.MessageIdentifier).ToList();
            delayedIds.ShouldContain(msg3.Id.MessageIdentifier);
            delayedIds.ShouldContain(msg4.Id.MessageIdentifier);
            
            // Double-check that all messages are accounted for
            var testQueueMessages = store.PersistedIncoming("test").ToList();
            testQueueMessages.Count.ShouldBe(2); // Only delayed messages
            
            var movedQueueMessages = store.PersistedIncoming("moved").ToList();
            movedQueueMessages.Count.ShouldBe(2); // Only moved messages
        }, TimeSpan.FromSeconds(5));
    }

    [Fact]
    public async Task BatchContext_ReceiveLaterSubsetThenSuccessfullyReceivedBatch()
    {
        await QueueScenarioAsync(async (queue, token) =>
        {
            var msg1 = NewMessage("test", "msg1");
            var msg2 = NewMessage("test", "msg2");
            var msg3 = NewMessage("test", "msg3");

            queue.Enqueue(msg1);
            queue.Enqueue(msg2);
            queue.Enqueue(msg3);

            var batchCtx = await queue.ReceiveBatch("test", cancellationToken: token).FirstAsync(token);
            batchCtx.Messages.Length.ShouldBe(3);

            // Delay msg1 using subset operation
            batchCtx.ReceiveLater(new[] { msg1.Id.MessageIdentifier }, TimeSpan.FromMilliseconds(800));

            // Then call SuccessfullyReceived on entire batch - should only affect msg2 and msg3
            batchCtx.SuccessfullyReceived();
            batchCtx.CommitChanges();

            var store = (LmdbMessageStore)queue.Store;

            // Queue should be empty immediately
            store.PersistedIncoming("test").Any().ShouldBeFalse();

            // Wait for delayed message to be re-enqueued
            await DeterministicDelayAsync(1000, token);

            // Only msg1 should reappear
            var delayedMessages = await queue.Receive("test", cancellationToken: token)
                .Take(1)
                .ToListAsync(token);
            delayedMessages.Count.ShouldBe(1);
            delayedMessages[0].Message.Id.MessageIdentifier.ShouldBe(msg1.Id.MessageIdentifier);

            // msg2 and msg3 should be gone
            store.PersistedIncoming("test").Count().ShouldBe(1);
            store.PersistedIncoming("test").Any(m => m.Id.MessageIdentifier == msg2.Id.MessageIdentifier).ShouldBeFalse();
            store.PersistedIncoming("test").Any(m => m.Id.MessageIdentifier == msg3.Id.MessageIdentifier).ShouldBeFalse();
        }, TimeSpan.FromSeconds(5));
    }

    [Fact]
    public async Task BatchContext_MoveToDeadLetterSubsetThenSuccessfullyReceivedBatch()
    {
        await QueueScenarioAsync(config =>config.WithDeadLetterQueue(),async (queue, token) =>
        {

            var msg1 = NewMessage("test", "msg1");
            var msg2 = NewMessage("test", "msg2");
            var msg3 = NewMessage("test", "msg3");

            queue.Enqueue(msg1);
            queue.Enqueue(msg2);
            queue.Enqueue(msg3);

            var batchCtx = await queue.ReceiveBatch("test", cancellationToken: token).FirstAsync(token);
            batchCtx.Messages.Length.ShouldBe(3);

            // Move msg1 to DLQ using subset operation
            batchCtx.MoveToDeadLetter(new[] { msg1.Id.MessageIdentifier });

            // Then call SuccessfullyReceived on entire batch - should only affect msg2 and msg3
            batchCtx.SuccessfullyReceived();
            batchCtx.CommitChanges();

            var store = (LmdbMessageStore)queue.Store;

            // Original queue should be empty
            store.PersistedIncoming("test").Any().ShouldBeFalse();

            // msg1 should be in DLQ
            var dlqMessages = store.PersistedIncoming(DeadLetterConstants.QueueName).ToList();
            dlqMessages.Count.ShouldBe(1);
            dlqMessages[0].Id.MessageIdentifier.ShouldBe(msg1.Id.MessageIdentifier);

            // msg2 and msg3 should be completely gone (successfully received)
            store.PersistedIncoming("test").Any(m => m.Id.MessageIdentifier == msg2.Id.MessageIdentifier).ShouldBeFalse();
            store.PersistedIncoming("test").Any(m => m.Id.MessageIdentifier == msg3.Id.MessageIdentifier).ShouldBeFalse();
            store.PersistedIncoming(DeadLetterConstants.QueueName).Any(m => m.Id.MessageIdentifier == msg2.Id.MessageIdentifier).ShouldBeFalse();
            store.PersistedIncoming(DeadLetterConstants.QueueName).Any(m => m.Id.MessageIdentifier == msg3.Id.MessageIdentifier).ShouldBeFalse();
        }, TimeSpan.FromSeconds(5));
    }
}