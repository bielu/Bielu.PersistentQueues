using System;
using System.Collections.Generic;
using LightningQueues.Storage.LMDB;

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
/// pending actions in a single atomic transaction — one store call per action type,
/// not one per message.
/// </remarks>
public class BatchQueueContext
{
    internal BatchQueueContext(Message[] messages, Queue queue)
    {
        Messages = messages;
        QueueContext = new BatchQueueContextImpl(messages, queue);
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
    /// in a single atomic transaction.
    /// </remarks>
    public IQueueContext QueueContext { get; }

    private class BatchQueueContextImpl : IQueueContext
    {
        private readonly Message[] _messages;
        private readonly Queue _queue;
        private readonly List<IBatchAction> _actions;

        internal BatchQueueContextImpl(Message[] messages, Queue queue)
        {
            _messages = messages;
            _queue = queue;
            _actions = new List<IBatchAction>();
        }

        public void CommitChanges()
        {
            if (_actions.Count == 0) return;

            using var transaction = _queue.Store.BeginTransaction();
            foreach (var action in _actions)
            {
                action.Execute(transaction);
            }
            transaction.Commit();

            foreach (var action in _actions)
            {
                action.Success();
            }
        }

        public void SuccessfullyReceived()
        {
            _actions.Add(new SuccessAllAction(_queue, _messages));
        }

        public void MoveTo(string queueName)
        {
            _actions.Add(new MoveAllAction(_queue, _messages, queueName));
        }

        public void Send(Message message)
        {
            _actions.Add(new SendAction(_queue, message));
        }

        public void Enqueue(Message message)
        {
            _actions.Add(new EnqueueAction(_queue, message));
        }

        public void ReceiveLater(TimeSpan timeSpan)
        {
            _actions.Add(new ReceiveLaterTimeSpanAction(_queue, _messages, timeSpan));
        }

        public void ReceiveLater(DateTimeOffset time)
        {
            _actions.Add(new ReceiveLaterDateTimeOffsetAction(_queue, _messages, time));
        }
    }

    private interface IBatchAction
    {
        void Execute(LmdbTransaction transaction);
        void Success();
    }

    private class SuccessAllAction : IBatchAction
    {
        private readonly Queue _queue;
        private readonly Message[] _messages;

        public SuccessAllAction(Queue queue, Message[] messages)
        {
            _queue = queue;
            _messages = messages;
        }

        public void Execute(LmdbTransaction transaction) =>
            _queue.Store.SuccessfullyReceived(transaction, _messages);

        public void Success() { }
    }

    private class MoveAllAction : IBatchAction
    {
        private readonly Queue _queue;
        private readonly Message[] _messages;
        private readonly string _queueName;

        public MoveAllAction(Queue queue, Message[] messages, string queueName)
        {
            _queue = queue;
            _messages = messages;
            _queueName = queueName;
        }

        public void Execute(LmdbTransaction transaction) =>
            _queue.Store.MoveToQueue(transaction, _queueName, _messages);

        public void Success() { }
    }

    private class SendAction : IBatchAction
    {
        private readonly Queue _queue;
        private readonly Message _message;

        public SendAction(Queue queue, Message message)
        {
            _queue = queue;
            _message = message;
        }

        public void Execute(LmdbTransaction transaction) =>
            _queue.Store.StoreOutgoing(transaction, _message);

        public void Success() { }
    }

    private class EnqueueAction : IBatchAction
    {
        private readonly Queue _queue;
        private readonly Message _message;

        public EnqueueAction(Queue queue, Message message)
        {
            _queue = queue;
            _message = message;
        }

        public void Execute(LmdbTransaction transaction) =>
            _queue.Store.StoreIncoming(transaction, _message);

        public void Success() { }
    }

    private class ReceiveLaterTimeSpanAction : IBatchAction
    {
        private readonly Queue _queue;
        private readonly Message[] _messages;
        private readonly TimeSpan _timeSpan;

        public ReceiveLaterTimeSpanAction(Queue queue, Message[] messages, TimeSpan timeSpan)
        {
            _queue = queue;
            _messages = messages;
            _timeSpan = timeSpan;
        }

        public void Execute(LmdbTransaction transaction) { }

        public void Success()
        {
            foreach (var message in _messages)
                _queue.ReceiveLater(message, _timeSpan);
        }
    }

    private class ReceiveLaterDateTimeOffsetAction : IBatchAction
    {
        private readonly Queue _queue;
        private readonly Message[] _messages;
        private readonly DateTimeOffset _time;

        public ReceiveLaterDateTimeOffsetAction(Queue queue, Message[] messages, DateTimeOffset time)
        {
            _queue = queue;
            _messages = messages;
            _time = time;
        }

        public void Execute(LmdbTransaction transaction) { }

        public void Success()
        {
            foreach (var message in _messages)
                _queue.ReceiveLater(message, _time);
        }
    }
}
