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
    private readonly BatchQueueContextImpl _impl;

    internal BatchQueueContext(Message[] messages, Queue queue)
    {
        Messages = messages;
        _impl = new BatchQueueContextImpl(messages, queue);
        QueueContext = _impl;
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

    /// <summary>
    /// Schedules a subset of messages in this batch to be processed again after a specified delay.
    /// </summary>
    /// <param name="messages">The messages to defer.</param>
    /// <param name="timeSpan">The time to wait before making the messages available again.</param>
    public void ReceiveLater(Message[] messages, TimeSpan timeSpan)
    {
        _impl.AddAction(new ReceiveLaterTimeSpanAction(_impl.Queue, messages, timeSpan));
    }

    /// <summary>
    /// Schedules a subset of messages in this batch to be processed again at a specific time.
    /// </summary>
    /// <param name="messages">The messages to defer.</param>
    /// <param name="time">The time at which the messages should be made available again.</param>
    public void ReceiveLater(Message[] messages, DateTimeOffset time)
    {
        _impl.AddAction(new ReceiveLaterDateTimeOffsetAction(_impl.Queue, messages, time));
    }

    /// <summary>
    /// Marks a subset of messages in this batch as successfully received and processed.
    /// </summary>
    /// <param name="messages">The messages to mark as received.</param>
    public void SuccessfullyReceived(Message[] messages)
    {
        _impl.AddAction(new SuccessAllAction(_impl.Queue, messages));
    }

    /// <summary>
    /// Moves a subset of messages in this batch to a different queue.
    /// </summary>
    /// <param name="queueName">The name of the destination queue.</param>
    /// <param name="messages">The messages to move.</param>
    public void MoveTo(string queueName, Message[] messages)
    {
        _impl.AddAction(new MoveAllAction(_impl.Queue, messages, queueName));
    }

    private class BatchQueueContextImpl : IQueueContext
    {
        private readonly Message[] _messages;
        private readonly List<IBatchAction> _actions;

        internal Queue Queue { get; }

        internal BatchQueueContextImpl(Message[] messages, Queue queue)
        {
            _messages = messages;
            Queue = queue;
            _actions = new List<IBatchAction>();
        }

        internal void AddAction(IBatchAction action) => _actions.Add(action);

        public void CommitChanges()
        {
            if (_actions.Count == 0) return;

            using var transaction = Queue.Store.BeginTransaction();
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
            _actions.Add(new SuccessAllAction(Queue, _messages));
        }

        public void MoveTo(string queueName)
        {
            _actions.Add(new MoveAllAction(Queue, _messages, queueName));
        }

        public void Send(Message message)
        {
            _actions.Add(new SendAction(Queue, message));
        }

        public void Enqueue(Message message)
        {
            _actions.Add(new EnqueueAction(Queue, message));
        }

        public void ReceiveLater(TimeSpan timeSpan)
        {
            _actions.Add(new ReceiveLaterTimeSpanAction(Queue, _messages, timeSpan));
        }

        public void ReceiveLater(DateTimeOffset time)
        {
            _actions.Add(new ReceiveLaterDateTimeOffsetAction(Queue, _messages, time));
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
