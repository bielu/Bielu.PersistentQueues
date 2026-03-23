using System;
using System.Collections.Generic;

namespace LightningQueues;

/// <summary>
/// Represents a batch of messages received from a queue, with a shared context
/// that commits all messages atomically.
/// </summary>
/// <remarks>
/// <see cref="BatchQueueContext"/> is yielded by <see cref="IQueue.ReceiveBatch"/>.
/// It exposes the full array of <see cref="Messages"/> in the batch and a single
/// <see cref="QueueContext"/> whose operations apply to every message in the batch.
/// Calling <see cref="IQueueContext.CommitChanges"/> on the context commits all
/// pending actions from all messages in a single atomic transaction.
/// </remarks>
public class BatchQueueContext
{
    private readonly IReadOnlyList<QueueContext> _perMessageContexts;

    internal BatchQueueContext(Message[] messages, Queue queue)
    {
        Messages = messages;
        var contexts = new List<QueueContext>(messages.Length);
        foreach (var message in messages)
        {
            contexts.Add(new QueueContext(queue, message));
        }
        _perMessageContexts = contexts;
        QueueContext = new BatchQueueContextImpl(contexts);
    }

    /// <summary>
    /// Gets the messages in this batch.
    /// </summary>
    public Message[] Messages { get; }

    /// <summary>
    /// Gets the queue context for the batch.
    /// </summary>
    /// <remarks>
    /// Operations such as <see cref="IQueueContext.SuccessfullyReceived"/> and
    /// <see cref="IQueueContext.MoveTo"/> apply to every message in the batch.
    /// <see cref="IQueueContext.CommitChanges"/> commits all pending actions
    /// from all messages in a single atomic transaction.
    /// </remarks>
    public IQueueContext QueueContext { get; }

    private class BatchQueueContextImpl : IQueueContext
    {
        private readonly IReadOnlyList<QueueContext> _contexts;

        internal BatchQueueContextImpl(IReadOnlyList<QueueContext> contexts)
        {
            _contexts = contexts;
        }

        public void CommitChanges()
        {
            LightningQueues.QueueContext.CommitBatch(_contexts);
        }

        public void Send(Message message)
        {
            if (_contexts.Count > 0)
                _contexts[0].Send(message);
        }

        public void ReceiveLater(TimeSpan timeSpan)
        {
            foreach (var ctx in _contexts)
                ctx.ReceiveLater(timeSpan);
        }

        public void ReceiveLater(DateTimeOffset time)
        {
            foreach (var ctx in _contexts)
                ctx.ReceiveLater(time);
        }

        public void SuccessfullyReceived()
        {
            foreach (var ctx in _contexts)
                ctx.SuccessfullyReceived();
        }

        public void MoveTo(string queueName)
        {
            foreach (var ctx in _contexts)
                ctx.MoveTo(queueName);
        }

        public void Enqueue(Message message)
        {
            if (_contexts.Count > 0)
                _contexts[0].Enqueue(message);
        }
    }
}
