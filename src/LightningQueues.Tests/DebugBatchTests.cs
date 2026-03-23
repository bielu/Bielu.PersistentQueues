// Debug test to investigate ReceiveBatch behavior
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using LightningQueues;
using LightningQueues.Storage.LMDB;
using Shouldly;

namespace LightningQueues.Tests;

public class DebugBatchTests : TestBase
{
    // Scenario 1: ReceiveBatch with no timeout, no maxMessages (defaults)
    public async Task debug_batch_no_timeout_no_limit()
    {
        await QueueScenario(async (queue, token) =>
        {
            queue.Enqueue(NewMessage("test", "msg1"));
            queue.Enqueue(NewMessage("test", "msg2"));
            
            Console?.WriteLine("[debug_batch_no_timeout_no_limit] Starting ReceiveBatch...");
            var sw = System.Diagnostics.Stopwatch.StartNew();
            
            var batchCtx = await queue.ReceiveBatch("test", cancellationToken: token).FirstAsync(token);
            
            sw.Stop();
            Console?.WriteLine($"[debug_batch_no_timeout_no_limit] Got batch with {batchCtx.Messages.Length} msgs in {sw.ElapsedMilliseconds}ms");
            batchCtx.Messages.Length.ShouldBe(2);
            
            batchCtx.QueueContext.SuccessfullyReceived();
            batchCtx.QueueContext.CommitChanges();
            Console?.WriteLine("[debug_batch_no_timeout_no_limit] CommitChanges succeeded");
        }, TimeSpan.FromSeconds(5));
    }
    
    // Scenario 2: ReceiveBatch with timeout, no maxMessages
    public async Task debug_batch_with_timeout()
    {
        await QueueScenario(async (queue, token) =>
        {
            queue.Enqueue(NewMessage("test", "msg1"));
            
            Console?.WriteLine("[debug_batch_with_timeout] Starting ReceiveBatch with 1000ms timeout...");
            var sw = System.Diagnostics.Stopwatch.StartNew();
            
            var batchCtx = await queue.ReceiveBatch("test", 
                    batchTimeoutInMilliseconds: 1000, 
                    pollIntervalInMilliseconds: 50, 
                    cancellationToken: token)
                .FirstAsync(token);
            
            sw.Stop();
            Console?.WriteLine($"[debug_batch_with_timeout] Got batch with {batchCtx.Messages.Length} msgs in {sw.ElapsedMilliseconds}ms");
            batchCtx.Messages.Length.ShouldBeGreaterThanOrEqualTo(1);
            
            batchCtx.QueueContext.SuccessfullyReceived();
            batchCtx.QueueContext.CommitChanges();
            Console?.WriteLine("[debug_batch_with_timeout] CommitChanges succeeded");
        }, TimeSpan.FromSeconds(5));
    }
    
    // Scenario 3: ReceiveBatch with maxMessages=1 (the user's Option 1)
    public async Task debug_batch_max_messages_1()
    {
        await QueueScenario(async (queue, token) =>
        {
            queue.Enqueue(NewMessage("test", "msg1"));
            
            Console?.WriteLine("[debug_batch_max_messages_1] Starting ReceiveBatch with maxMessages=1...");
            var sw = System.Diagnostics.Stopwatch.StartNew();
            
            var batchCtx = await queue.ReceiveBatch("test", maxMessages: 1, cancellationToken: token).FirstAsync(token);
            
            sw.Stop();
            Console?.WriteLine($"[debug_batch_max_messages_1] Got batch with {batchCtx.Messages.Length} msgs in {sw.ElapsedMilliseconds}ms");
            batchCtx.Messages.Length.ShouldBe(1);
            
            batchCtx.QueueContext.SuccessfullyReceived();
            batchCtx.QueueContext.CommitChanges();
            Console?.WriteLine("[debug_batch_max_messages_1] CommitChanges succeeded");
        }, TimeSpan.FromSeconds(5));
    }
    
    // Scenario 4: await foreach loop pattern (mimicking user's code) with ReceiveBatch  
    public async Task debug_batch_await_foreach_loop()
    {
        await QueueScenario(async (queue, token) =>
        {
            queue.Enqueue(NewMessage("test", "msg1"));
            queue.Enqueue(NewMessage("test", "msg2"));
            queue.Enqueue(NewMessage("test", "msg3"));
            
            Console?.WriteLine("[debug_batch_await_foreach_loop] Starting await foreach over ReceiveBatch...");
            
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(cts.Token, token);
            
            int totalProcessed = 0;
            int batchCount = 0;
            await foreach (var batchCtx in queue.ReceiveBatch("test", maxMessages: 2, 
                pollIntervalInMilliseconds: 50, cancellationToken: linked.Token))
            {
                batchCount++;
                Console?.WriteLine($"[debug_batch_await_foreach_loop] Batch {batchCount}: {batchCtx.Messages.Length} messages");
                
                foreach (var msg in batchCtx.Messages)
                {
                    var payload = Encoding.UTF8.GetString(msg.DataArray!);
                    Console?.WriteLine($"  - Message: {payload}");
                    totalProcessed++;
                }
                
                batchCtx.QueueContext.SuccessfullyReceived();
                batchCtx.QueueContext.CommitChanges();
                Console?.WriteLine($"[debug_batch_await_foreach_loop] Batch {batchCount} committed");
                
                if (totalProcessed >= 3)
                    break;
            }
            
            Console?.WriteLine($"[debug_batch_await_foreach_loop] Done: {totalProcessed} messages in {batchCount} batches");
            totalProcessed.ShouldBe(3);
        }, TimeSpan.FromSeconds(5));
    }
    
    // Scenario 5: ReceiveBatch with timeout in await foreach loop
    public async Task debug_batch_timeout_await_foreach_loop()
    {
        await QueueScenario(async (queue, token) =>
        {
            queue.Enqueue(NewMessage("test", "msg1"));
            
            // Enqueue second message after a delay
            _ = Task.Run(async () =>
            {
                await DeterministicDelay(300, token);
                Console?.WriteLine("[debug_batch_timeout_await_foreach_loop] Enqueuing msg2 after delay");
                queue.Enqueue(NewMessage("test", "msg2"));
            }, token);
            
            Console?.WriteLine("[debug_batch_timeout_await_foreach_loop] Starting await foreach with timeout...");
            var sw = System.Diagnostics.Stopwatch.StartNew();
            
            var batchCtx = await queue.ReceiveBatch("test", 
                    batchTimeoutInMilliseconds: 1000, 
                    pollIntervalInMilliseconds: 50, 
                    cancellationToken: token)
                .FirstAsync(token);
            
            sw.Stop();
            Console?.WriteLine($"[debug_batch_timeout_await_foreach_loop] Got {batchCtx.Messages.Length} msgs in {sw.ElapsedMilliseconds}ms");
            
            // Should have collected both messages within the timeout window
            batchCtx.Messages.Length.ShouldBe(2);
            
            batchCtx.QueueContext.SuccessfullyReceived();
            batchCtx.QueueContext.CommitChanges();
        }, TimeSpan.FromSeconds(5));
    }
}
