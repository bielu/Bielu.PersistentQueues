using System;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;

namespace Bielu.PersistentQueues.Examples.Services;

/// <summary>
/// Demonstrates the Dead Letter Queue (DLQ) feature:
///   1. Enqueues messages with a low MaxAttempts limit.
///   2. Simulates repeated processing failures via ReceiveLater.
///   3. Messages automatically land in the DLQ after exceeding MaxAttempts.
///   4. Shows how to explicitly move a "poison" message to the DLQ.
///   5. Lists DLQ contents and requeues them back to the original queue.
/// </summary>
public class DeadLetterDemoService(IQueue queue) : BackgroundService
{
    private readonly IQueue _queue = queue;
    private const string QueueName = "dlq-demo";

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        Console.WriteLine();
        Console.WriteLine("──────────────────────────────────────────────────────");
        Console.WriteLine(" Dead Letter Queue Demo");
        Console.WriteLine("──────────────────────────────────────────────────────");
        Console.WriteLine();

        // ── 1. Enqueue messages with MaxAttempts = 2 ─────────────────────────
        Console.WriteLine("[1] Enqueuing 3 messages with MaxAttempts = 2 …");
        for (var i = 1; i <= 3; i++)
        {
            var msg = Message.Create(
                data: Encoding.UTF8.GetBytes($"order-{i}"),
                queue: QueueName,
                maxAttempts: 2);
            _queue.Enqueue(msg);
        }
        Console.WriteLine("    Done.");
        Console.WriteLine();

        // ── 2. Simulate processing failures ──────────────────────────────────
        Console.WriteLine("[2] Consuming messages – simulating failures via ReceiveLater …");

        // First pass: ReceiveLater bumps ProcessingAttempts to 1
        using var cts1 = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        cts1.CancelAfter(TimeSpan.FromSeconds(2));
        try
        {
            await foreach (var ctx in _queue.Receive(QueueName, cancellationToken: cts1.Token).ConfigureAwait(false))
            {
                Console.WriteLine($"    [{ctx.Message.ProcessingAttempts + 1}/2] Processing '{Encoding.UTF8.GetString(ctx.Message.Data.Span)}' … failing.");
                ctx.QueueContext.ReceiveLater(TimeSpan.FromMilliseconds(50));
                ctx.QueueContext.CommitChanges();
            }
        }
        catch (OperationCanceledException) { /* expected */ }

        // Short delay for scheduled messages to reappear
        await Task.Delay(200, stoppingToken).ConfigureAwait(false);

        // Second pass: ReceiveLater bumps ProcessingAttempts to 2 → triggers auto-DLQ
        Console.WriteLine("    Second failure attempt – messages should land in DLQ …");
        using var cts2 = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        cts2.CancelAfter(TimeSpan.FromSeconds(2));
        try
        {
            await foreach (var ctx in _queue.Receive(QueueName, cancellationToken: cts2.Token).ConfigureAwait(false))
            {
                Console.WriteLine($"    [{ctx.Message.ProcessingAttempts + 1}/2] Processing '{Encoding.UTF8.GetString(ctx.Message.Data.Span)}' … failing → auto-DLQ.");
                ctx.QueueContext.ReceiveLater(TimeSpan.FromMilliseconds(50));
                ctx.QueueContext.CommitChanges();
            }
        }
        catch (OperationCanceledException) { /* expected */ }

        Console.WriteLine();

        // ── 3. Explicit MoveToDeadLetter ─────────────────────────────────────
        Console.WriteLine("[3] Enqueuing a poison message and explicitly dead-lettering it …");
        _queue.Enqueue(Message.Create(
            data: Encoding.UTF8.GetBytes("poison-pill"),
            queue: QueueName));

        using var cts3 = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        cts3.CancelAfter(TimeSpan.FromSeconds(2));
        try
        {
            await foreach (var ctx in _queue.Receive(QueueName, cancellationToken: cts3.Token).ConfigureAwait(false))
            {
                Console.WriteLine($"    Received '{Encoding.UTF8.GetString(ctx.Message.Data.Span)}' → calling MoveToDeadLetter()");
                ctx.QueueContext.MoveToDeadLetter();
                ctx.QueueContext.CommitChanges();
            }
        }
        catch (OperationCanceledException) { /* expected */ }

        Console.WriteLine();

        // ── 4. Inspect the DLQ ───────────────────────────────────────────────
        var dlqName = DeadLetterConstants.QueueName;
        var dlqMessages = _queue.Store.PersistedIncoming(dlqName).ToList();

        Console.WriteLine($"[4] Dead letter queue '{dlqName}' contains {dlqMessages.Count} message(s):");
        foreach (var m in dlqMessages)
        {
            Console.WriteLine($"    • '{Encoding.UTF8.GetString(m.Data.Span)}'  " +
                              $"originalQueue={m.OriginalQueue}  " +
                              $"attempts={m.ProcessingAttempts}");
        }
        Console.WriteLine();

        // ── 5. Requeue all messages from the DLQ ─────────────────────────────
        Console.WriteLine("[5] Requeuing all DLQ messages back to original queue …");
        var requeued = _queue.RequeueDeadLetterMessages();
        Console.WriteLine($"    Requeued {requeued} message(s).");

        var remaining = _queue.Store.PersistedIncoming(dlqName).Count();
        var backInQueue = _queue.Store.PersistedIncoming(QueueName).Count();
        Console.WriteLine($"    DLQ now has {remaining} message(s), '{QueueName}' has {backInQueue} message(s).");
        Console.WriteLine();

        Console.WriteLine("──────────────────────────────────────────────────────");
        Console.WriteLine(" Dead Letter Queue Demo Complete");
        Console.WriteLine("──────────────────────────────────────────────────────");
        Console.WriteLine();
    }
}
