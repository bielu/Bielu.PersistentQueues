using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using LightningQueues.Logging;
using LightningQueues.Serialization;
using LightningQueues.Storage.LMDB;
using Shouldly;

namespace LightningQueues.Tests;

public class QueueTests : TestBase
{
    public async Task receive_at_a_later_time()
    {
        await QueueScenario(async (queue, token) =>
        {
            queue.ReceiveLater(NewMessage("test"), TimeSpan.FromSeconds(1));
            var receiveTask = queue.Receive("test", cancellationToken: token).FirstAsync(token);
            await DeterministicDelay(100, token);
            receiveTask.IsCompleted.ShouldBeFalse();
            await DeterministicDelay(2000, token);
            receiveTask.IsCompleted.ShouldBeTrue();
        }, TimeSpan.FromSeconds(4));
    }

    public async Task receive_at_a_specified_time()
    {
        await QueueScenario(async (queue, token) =>
        {
            queue.ReceiveLater(NewMessage("test"), DateTimeOffset.Now.AddSeconds(1));
            var receiveTask = queue.Receive("test", cancellationToken: token).FirstAsync(token);
            await DeterministicDelay(100, token);
            receiveTask.IsCompleted.ShouldBeFalse();
            await DeterministicDelay(2000, token);
            receiveTask.IsCompleted.ShouldBeTrue();
        }, TimeSpan.FromSeconds(4));
    }

    public async Task enqueue_a_message()
    {
        await QueueScenario(async (queue, token) =>
        {
            var expected = NewMessage("test");
            var receiveTask = queue.Receive("test", cancellationToken: token).FirstAsync(token);
            queue.Enqueue(expected);
            var result = await receiveTask;
            result.Message.Id.ShouldBe(expected.Id);
            result.Message.QueueString.ShouldBe(expected.QueueString);
            result.Message.DataArray.ShouldBe(expected.DataArray);
        });
    }

    public async Task moving_queues()
    {
        await QueueScenario(async (queue, token) =>
        {
            queue.CreateQueue("another");
            var expected = NewMessage("test");
            queue.Enqueue(expected);
            var message = await queue.Receive("test", 50, cancellationToken: token).FirstAsync(token);
            queue.MoveToQueue("another", message.Message);

            message = await queue.Receive("another", 50, cancellationToken: token).FirstAsync(token);
            message.Message.QueueString.ShouldBe("another");
        });
    }

    public async Task send_message_to_self()
    {
        await QueueScenario(async (queue, token) =>
        {
            var message = Message.Create(
                data: "hello"u8.ToArray(),
                queue: "test",
                destinationUri: $"lq.tcp://localhost:{queue.Endpoint.Port}"
            );
            queue.Send(message);
            var received = await queue.Receive("test", cancellationToken: token).FirstAsync(token);
            received.ShouldNotBeNull();
            received.Message.Queue.ShouldBe(message.Queue);
            received.Message.Data.ShouldBe(message.Data);
        });
    }

    public async Task sending_to_bad_endpoint_no_retries_integration_test()
    {
        await QueueScenario(config =>
            {
                config.TimeoutNetworkBatchAfter(TimeSpan.FromSeconds(1));
            },
            async (queue, token) =>
            {
                var message = Message.Create(
                    data: "hello"u8.ToArray(),
                    queue: "test",
                    destinationUri: $"lq.tcp://boom:{queue.Endpoint.Port + 1}",
                    maxAttempts: 1
                );
                queue.Send(message);
                await DeterministicDelay(5000, token); //connect timeout cancellation, but windows is slow
                var store = (LmdbMessageStore)queue.Store;
                store.PersistedOutgoing().Any().ShouldBeFalse();
            }, TimeSpan.FromSeconds(10));
    }

    public async Task can_start_two_instances_for_IIS_stop_and_start()
    {
        //This shows that the port doesn't have an exclusive lock, and that lmdb itself can have multiple instances
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var serializer = new MessageSerializer();
        using var env = LightningEnvironment();
        using var store = new LmdbMessageStore(env, serializer);
        var queueConfiguration = new QueueConfiguration();
        queueConfiguration.WithDefaults();
        queueConfiguration.LogWith(new RecordingLogger(Console));
        queueConfiguration.AutomaticEndpoint();
        queueConfiguration.SerializeWith(serializer);
        queueConfiguration.StoreMessagesWith(() => store);
        using var queue = queueConfiguration.BuildQueue();
        using var queue2 = queueConfiguration.BuildQueue();
        queue.CreateQueue("test");
        queue.Start();
        queue2.CreateQueue("test");
        queue2.Start();
        await cancellation.CancelAsync();
    }
    
    public async Task send_batch_of_messages()
    {
        await QueueScenario(async (queue, token) =>
        {
            // Create batch of messages for self
            var destination = $"lq.tcp://localhost:{queue.Endpoint.Port}";
            var message1 = Message.Create(
                data: "payload1"u8.ToArray(),
                queue: "test",
                destinationUri: destination
            );
            var message2 = Message.Create(
                data: "payload2"u8.ToArray(),
                queue: "test",
                destinationUri: destination
            );
            var message3 = Message.Create(
                data: "payload3"u8.ToArray(),
                queue: "test",
                destinationUri: destination
            );
            
            // Send all messages as a batch
            queue.Send(message1, message2, message3);
            
            // Receive all the messages (should be 3)
            var receivedMessages = await queue.Receive("test", cancellationToken: token)
                .Take(3)
                .ToListAsync(token);
            
            receivedMessages.Count.ShouldBe(3);
            
            // Verify we received each message
            var payloads = receivedMessages
                .Select(ctx => System.Text.Encoding.UTF8.GetString(ctx.Message.DataArray!))
                .ToList();
                
            payloads.ShouldContain("payload1");
            payloads.ShouldContain("payload2");
            payloads.ShouldContain("payload3");
            await DeterministicDelay(TimeSpan.FromSeconds(1), token);
            
            // Verify the message store shows they were all sent
            var store = (LmdbMessageStore)queue.Store;
            store.PersistedOutgoing().Count().ShouldBe(0); // Should be 0 since they were processed
        }, TimeSpan.FromSeconds(5));
    }
    
    public async Task receive_from_multiple_queues_concurrently()
    {
        await QueueScenario(async (queue, token) =>
        {
            // Create multiple queues
            queue.CreateQueue("queue1");
            queue.CreateQueue("queue2");
            queue.CreateQueue("queue3");
            
            // Enqueue messages in different queues
            var message1 = NewMessage("queue1", "payload1");
            var message2 = NewMessage("queue2", "payload2");
            var message3 = NewMessage("queue3", "payload3");
            
            queue.Enqueue(message1);
            queue.Enqueue(message2);
            queue.Enqueue(message3);
            
            // Start receiving from all queues concurrently with fast polling
            var receiveQueue1Task = queue.Receive("queue1", pollIntervalInMilliseconds: 10, cancellationToken: token).FirstAsync(token);
            var receiveQueue2Task = queue.Receive("queue2", pollIntervalInMilliseconds: 10, cancellationToken: token).FirstAsync(token);
            var receiveQueue3Task = queue.Receive("queue3", pollIntervalInMilliseconds: 10, cancellationToken: token).FirstAsync(token);
            
            // Wait for all receives to complete and get results
            var received1 = await receiveQueue1Task;
            var received2 = await receiveQueue2Task;
            var received3 = await receiveQueue3Task;
            
            System.Text.Encoding.UTF8.GetString(received1.Message.DataArray!).ShouldBe("payload1");
            System.Text.Encoding.UTF8.GetString(received2.Message.DataArray!).ShouldBe("payload2");
            System.Text.Encoding.UTF8.GetString(received3.Message.DataArray!).ShouldBe("payload3");
            
            received1.Message.QueueString.ShouldBe("queue1");
            received2.Message.QueueString.ShouldBe("queue2");
            received3.Message.QueueString.ShouldBe("queue3");
        }, TimeSpan.FromSeconds(5));
    }
    
    public async Task get_all_queue_names()
    {
        await QueueScenario((queue, token) =>
        {
            // Create multiple queues
            queue.CreateQueue("queue1");
            queue.CreateQueue("queue2");
            queue.CreateQueue("queue3");
            
            // Get all queue names
            var queueNames = queue.Queues;
            
            // Verify all queues are listed (including the default "test" queue)
            queueNames.Length.ShouldBe(4); 
            queueNames.ShouldContain("test");
            queueNames.ShouldContain("queue1");
            queueNames.ShouldContain("queue2");
            queueNames.ShouldContain("queue3");
            
            return Task.CompletedTask;
        });
    }
    
    public async Task receive_batch_of_enqueued_messages()
    {
        await QueueScenario(async (queue, token) =>
        {
            queue.Enqueue(NewMessage("test", "msg1"));
            queue.Enqueue(NewMessage("test", "msg2"));
            queue.Enqueue(NewMessage("test", "msg3"));
            
            var batchCtx = await queue.ReceiveBatch("test", cancellationToken: token).FirstAsync(token);
            
            batchCtx.Messages.Length.ShouldBe(3);
            var payloads = batchCtx.Messages.Select(m => System.Text.Encoding.UTF8.GetString(m.DataArray!)).ToList();
            payloads.ShouldContain("msg1");
            payloads.ShouldContain("msg2");
            payloads.ShouldContain("msg3");
        }, TimeSpan.FromSeconds(5));
    }
    
    public async Task receive_batch_with_max_messages_limit()
    {
        await QueueScenario(async (queue, token) =>
        {
            queue.Enqueue(NewMessage("test", "msg1"));
            queue.Enqueue(NewMessage("test", "msg2"));
            queue.Enqueue(NewMessage("test", "msg3"));
            
            var batchCtx = await queue.ReceiveBatch("test", maxMessages: 2, cancellationToken: token).FirstAsync(token);
            
            batchCtx.Messages.Length.ShouldBe(2);
        }, TimeSpan.FromSeconds(5));
    }
    
    public async Task receive_batch_returns_empty_on_timeout_when_no_messages()
    {
        await QueueScenario(async (queue, token) =>
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(500));
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(cts.Token, token);
            
            var batches = await queue.ReceiveBatch("test", cancellationToken: linked.Token)
                .ToListAsync(linked.Token)
                .AsTask()
                .ContinueWith(t => t.IsCompletedSuccessfully ? t.Result : new List<IBatchQueueContext>());
            
            batches.Count.ShouldBe(0);
        }, TimeSpan.FromSeconds(3));
    }
    
    public async Task receive_batch_yields_multiple_batches_as_messages_arrive()
    {
        await QueueScenario(async (queue, token) =>
        {
            queue.Enqueue(NewMessage("test", "first"));
            
            var batches = new List<IBatchQueueContext>();
            var batchEnumerator = queue.ReceiveBatch("test", pollIntervalInMilliseconds: 50, cancellationToken: token)
                .GetAsyncEnumerator(token);
            
            // First batch should contain the already-enqueued message
            (await batchEnumerator.MoveNextAsync()).ShouldBeTrue();
            batches.Add(batchEnumerator.Current);
            batches[0].Messages.Length.ShouldBe(1);
            System.Text.Encoding.UTF8.GetString(batches[0].Messages[0].DataArray!).ShouldBe("first");
            
            // Acknowledge first batch so it's removed from storage, then enqueue more
            batches[0].SuccessfullyReceived();
            batches[0].CommitChanges();
            
            queue.Enqueue(NewMessage("test", "second"));
            (await batchEnumerator.MoveNextAsync()).ShouldBeTrue();
            batches.Add(batchEnumerator.Current);
            batches[1].Messages.Length.ShouldBe(1);
            System.Text.Encoding.UTF8.GetString(batches[1].Messages[0].DataArray!).ShouldBe("second");
            
            await batchEnumerator.DisposeAsync();
        }, TimeSpan.FromSeconds(5));
    }
    
    public async Task receive_batch_respects_cancellation_token()
    {
        await QueueScenario(async (queue, token) =>
        {
            queue.Enqueue(NewMessage("test", "msg1"));
            
            using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(500));
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(cts.Token, token);
            
            var batches = new List<IBatchQueueContext>();
            await foreach (var batchCtx in queue.ReceiveBatch("test", pollIntervalInMilliseconds: 50, cancellationToken: linked.Token))
            {
                batches.Add(batchCtx);
            }
            
            batches.Count.ShouldBeGreaterThanOrEqualTo(1);
            batches[0].Messages.Length.ShouldBe(1);
        }, TimeSpan.FromSeconds(3));
    }
    
    public async Task receive_batch_with_timeout_collects_messages_over_window()
    {
        await QueueScenario(async (queue, token) =>
        {
            // Enqueue first message immediately
            queue.Enqueue(NewMessage("test", "msg1"));
            
            // Enqueue second message after 200ms — within the 500ms timeout window
            _ = Task.Run(async () =>
            {
                await DeterministicDelay(200, token);
                queue.Enqueue(NewMessage("test", "msg2"));
            }, token);
            
            // Use a 500ms timeout. The method should wait for the full window,
            // collecting msg1 immediately and msg2 when it arrives at ~200ms.
            var sw = System.Diagnostics.Stopwatch.StartNew();
            var batchCtx = await queue.ReceiveBatch("test", batchTimeoutInMilliseconds: 500, 
                    pollIntervalInMilliseconds: 50, cancellationToken: token)
                .FirstAsync(token);
            sw.Stop();
            
            batchCtx.Messages.Length.ShouldBe(2);
            var payloads = batchCtx.Messages.Select(m => System.Text.Encoding.UTF8.GetString(m.DataArray!)).ToList();
            payloads.ShouldContain("msg1");
            payloads.ShouldContain("msg2");
            // Should have waited for the full timeout window (~500ms), not yielded early
            sw.ElapsedMilliseconds.ShouldBeGreaterThanOrEqualTo(400);
        }, TimeSpan.FromSeconds(5));
    }
    
    public async Task receive_batch_without_timeout_yields_immediately()
    {
        await QueueScenario(async (queue, token) =>
        {
            // Enqueue one message
            queue.Enqueue(NewMessage("test", "msg1"));
            
            // Without timeout (default), should yield immediately with only the first message
            var batchCtx = await queue.ReceiveBatch("test", pollIntervalInMilliseconds: 50, 
                    cancellationToken: token)
                .FirstAsync(token);
            
            // Should get the message immediately without waiting
            batchCtx.Messages.Length.ShouldBeGreaterThanOrEqualTo(1);
        }, TimeSpan.FromSeconds(3));
    }
    
    public async Task receive_batch_timeout_does_not_yield_empty_batches()
    {
        await QueueScenario(async (queue, token) =>
        {
            // With a short timeout and no messages, the stream should not yield empty batches
            using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(600));
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(cts.Token, token);
            
            var batches = new List<IBatchQueueContext>();
            await foreach (var batchCtx in queue.ReceiveBatch("test", batchTimeoutInMilliseconds: 200, 
                pollIntervalInMilliseconds: 50, cancellationToken: linked.Token))
            {
                batches.Add(batchCtx);
            }
            
            // No messages were enqueued, so no batches should have been yielded
            batches.Count.ShouldBe(0);
        }, TimeSpan.FromSeconds(3));
    }
    
    public async Task receive_batch_timeout_with_max_messages_yields_early_when_full()
    {
        await QueueScenario(async (queue, token) =>
        {
            // Enqueue 3 messages
            queue.Enqueue(NewMessage("test", "msg1"));
            queue.Enqueue(NewMessage("test", "msg2"));
            queue.Enqueue(NewMessage("test", "msg3"));
            
            // Set a long timeout but a low max - should yield as soon as max is reached
            var start = DateTime.UtcNow;
            var batchCtx = await queue.ReceiveBatch("test", maxMessages: 2, 
                    batchTimeoutInMilliseconds: 5000, pollIntervalInMilliseconds: 50, 
                    cancellationToken: token)
                .FirstAsync(token);
            var elapsed = DateTime.UtcNow - start;
            
            batchCtx.Messages.Length.ShouldBe(2);
            // Should have returned well before the 5-second timeout
            elapsed.TotalMilliseconds.ShouldBeLessThan(2000);
        }, TimeSpan.FromSeconds(5));
    }
    
    public async Task receive_batch_timeout_alone_waits_for_window()
    {
        await QueueScenario(async (queue, token) =>
        {
            // Enqueue a message immediately
            queue.Enqueue(NewMessage("test", "msg1"));
            
            // With timeout only (no maxMessages), the batch should wait for the
            // full timeout window — not yield immediately when messages are found.
            var sw = System.Diagnostics.Stopwatch.StartNew();
            var batchCtx = await queue.ReceiveBatch("test", batchTimeoutInMilliseconds: 300, 
                    pollIntervalInMilliseconds: 50, cancellationToken: token)
                .FirstAsync(token);
            sw.Stop();
            
            batchCtx.Messages.Length.ShouldBe(1);
            // Should have waited ~300ms for the timeout window, not returned instantly
            sw.ElapsedMilliseconds.ShouldBeGreaterThanOrEqualTo(250);
        }, TimeSpan.FromSeconds(5));
    }
    
    public async Task receive_batch_timeout_collects_late_arriving_messages()
    {
        await QueueScenario(async (queue, token) =>
        {
            // Enqueue messages at different times within the timeout window
            queue.Enqueue(NewMessage("test", "msg1"));
            
            _ = Task.Run(async () =>
            {
                await DeterministicDelay(100, token);
                queue.Enqueue(NewMessage("test", "msg2"));
                await DeterministicDelay(100, token);
                queue.Enqueue(NewMessage("test", "msg3"));
            }, token);
            
            // 500ms timeout should collect all 3 messages that arrive over ~200ms
            var batchCtx = await queue.ReceiveBatch("test", batchTimeoutInMilliseconds: 500, 
                    pollIntervalInMilliseconds: 50, cancellationToken: token)
                .FirstAsync(token);
            
            batchCtx.Messages.Length.ShouldBe(3);
            var payloads = batchCtx.Messages.Select(m => System.Text.Encoding.UTF8.GetString(m.DataArray!)).ToList();
            payloads.ShouldContain("msg1");
            payloads.ShouldContain("msg2");
            payloads.ShouldContain("msg3");
        }, TimeSpan.FromSeconds(5));
    }
    
    public async Task receive_batch_either_max_or_timeout_whichever_first()
    {
        await QueueScenario(async (queue, token) =>
        {
            // Only 1 message available — maxMessages=30 won't be reached,
            // so timeout (300ms) should trigger the yield.
            queue.Enqueue(NewMessage("test", "msg1"));
            
            var sw = System.Diagnostics.Stopwatch.StartNew();
            var batchCtx = await queue.ReceiveBatch("test", maxMessages: 30, 
                    batchTimeoutInMilliseconds: 300, pollIntervalInMilliseconds: 50, 
                    cancellationToken: token)
                .FirstAsync(token);
            sw.Stop();
            
            batchCtx.Messages.Length.ShouldBe(1);
            // Timeout should have triggered — not returned early and not waited forever
            sw.ElapsedMilliseconds.ShouldBeGreaterThanOrEqualTo(250);
            sw.ElapsedMilliseconds.ShouldBeLessThan(1000);
        }, TimeSpan.FromSeconds(5));
    }
    
    public async Task receive_batch_receive_later_subset_of_messages()
    {
        await QueueScenario(async (queue, token) =>
        {
            queue.Enqueue(NewMessage("test", "process-now"));
            queue.Enqueue(NewMessage("test", "defer-me"));
            queue.Enqueue(NewMessage("test", "also-process"));
            
            // Get the batch
            var batchCtx = await queue.ReceiveBatch("test", cancellationToken: token).FirstAsync(token);
            batchCtx.Messages.Length.ShouldBe(3);
            
            // Split messages: defer one, mark the rest as received
            var toDefer = batchCtx.Messages.Where(m => 
                System.Text.Encoding.UTF8.GetString(m.DataArray!) == "defer-me").ToArray();
            var toProcess = batchCtx.Messages.Where(m => 
                System.Text.Encoding.UTF8.GetString(m.DataArray!) != "defer-me").ToArray();
            
            toDefer.Length.ShouldBe(1);
            toProcess.Length.ShouldBe(2);
            
            // Defer one message, successfully receive the others
            batchCtx.ReceiveLater(toDefer, TimeSpan.FromMilliseconds(500));
            batchCtx.SuccessfullyReceived(toProcess);
            batchCtx.CommitChanges();
            
            // The deferred message should reappear after the delay
            var reappeared = await queue.Receive("test", cancellationToken: token).FirstAsync(token);
            System.Text.Encoding.UTF8.GetString(reappeared.Message.DataArray!).ShouldBe("defer-me");
        }, TimeSpan.FromSeconds(5));
    }
    
    public async Task receive_batch_receive_later_subset_with_datetimeoffset()
    {
        await QueueScenario(async (queue, token) =>
        {
            queue.Enqueue(NewMessage("test", "process-now"));
            queue.Enqueue(NewMessage("test", "defer-me"));
            
            var batchCtx = await queue.ReceiveBatch("test", cancellationToken: token).FirstAsync(token);
            batchCtx.Messages.Length.ShouldBe(2);
            
            var toDefer = batchCtx.Messages.Where(m => 
                System.Text.Encoding.UTF8.GetString(m.DataArray!) == "defer-me").ToArray();
            var toProcess = batchCtx.Messages.Where(m => 
                System.Text.Encoding.UTF8.GetString(m.DataArray!) != "defer-me").ToArray();
            
            batchCtx.ReceiveLater(toDefer, DateTimeOffset.Now.AddMilliseconds(500));
            batchCtx.SuccessfullyReceived(toProcess);
            batchCtx.CommitChanges();
            
            // The deferred message should reappear after the specified time
            var reappeared = await queue.Receive("test", cancellationToken: token).FirstAsync(token);
            System.Text.Encoding.UTF8.GetString(reappeared.Message.DataArray!).ShouldBe("defer-me");
        }, TimeSpan.FromSeconds(5));
    }
    
    public async Task receive_batch_move_subset_of_messages()
    {
        await QueueScenario(async (queue, token) =>
        {
            queue.CreateQueue("other");
            queue.Enqueue(NewMessage("test", "stay"));
            queue.Enqueue(NewMessage("test", "move-me"));
            
            var batchCtx = await queue.ReceiveBatch("test", cancellationToken: token).FirstAsync(token);
            batchCtx.Messages.Length.ShouldBe(2);
            
            var toMove = batchCtx.Messages.Where(m =>
                System.Text.Encoding.UTF8.GetString(m.DataArray!) == "move-me").ToArray();
            var toKeep = batchCtx.Messages.Where(m =>
                System.Text.Encoding.UTF8.GetString(m.DataArray!) != "move-me").ToArray();
            
            batchCtx.MoveTo("other", toMove);
            batchCtx.SuccessfullyReceived(toKeep);
            batchCtx.CommitChanges();
            
            // The moved message should appear in the "other" queue
            var moved = await queue.Receive("other", cancellationToken: token).FirstAsync(token);
            System.Text.Encoding.UTF8.GetString(moved.Message.DataArray!).ShouldBe("move-me");
        }, TimeSpan.FromSeconds(5));
    }
}