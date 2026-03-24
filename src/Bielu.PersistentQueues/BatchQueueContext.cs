using System;
using System.Collections.Generic;
using Bielu.PersistentQueues.Storage;

namespace Bielu.PersistentQueues;

/// <summary>
/// Represents a batch of messages received from a queue, with a shared context
/// that commits all messages atomically.
/// </summary>
/// <remarks>
/// <see cref="BatchQueueContext"/> is yielded by <see cref="IQueue.ReceiveBatch"/>.
/// It exposes the full array of <see cref="Messages"/> in the batch.
/// Operations inherited from <see cref="IQueueContext"/> (such as
/// <see cref="IQueueContext.SuccessfullyReceived"/> and <see cref="IQueueContext.MoveTo"/>)
/// apply to every message in the batch. Subset operations from
/// <see cref="IBatchQueueContext"/> target specific messages.
/// Calling <see cref="IQueueContext.CommitChanges"/> commits all
/// pending actions in a single atomic transaction — one store call per action type,
/// not one per message.
/// </remarks>
public class BatchQueueContext : IBatchQueueContext
{
    private readonly Queue _queue;
    private readonly List<IBatchAction> _actions;

    internal BatchQueueContext(Message[] messages, Queue queue)
    {
        Messages = messages;
        _queue = queue;
        _actions = new List<IBatchAction>();
    }

    /// <inheritdoc />
    public Message[] Messages { get; }

    /// <inheritdoc />
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

    /// <inheritdoc />
    public void Send(Message message)
    {
        _actions.Add(new SendAction(_queue, message));
    }

    /// <inheritdoc />
    public void Enqueue(Message message)
    {
        _actions.Add(new EnqueueAction(_queue, message));
    }

    /// <inheritdoc />
    public void ReceiveLater(TimeSpan timeSpan)
    {
        _actions.Add(new ReceiveLaterTimeSpanAction(_queue, Messages, timeSpan));
    }

    /// <inheritdoc />
    public void ReceiveLater(DateTimeOffset time)
    {
        _actions.Add(new ReceiveLaterDateTimeOffsetAction(_queue, Messages, time));
    }

    /// <inheritdoc />
    public void SuccessfullyReceived()
    {
        _actions.Add(new SuccessAllAction(_queue, Messages));
    }

    /// <inheritdoc />
    public void MoveTo(string queueName)
    {
        _actions.Add(new MoveAllAction(_queue, Messages, queueName));
    }

    /// <inheritdoc />
    public void ReceiveLater(Message[] messages, TimeSpan timeSpan)
    {
        _actions.Add(new ReceiveLaterTimeSpanAction(_queue, messages, timeSpan));
    }

    /// <inheritdoc />
    public void ReceiveLater(Message[] messages, DateTimeOffset time)
    {
        _actions.Add(new ReceiveLaterDateTimeOffsetAction(_queue, messages, time));
    }

    /// <inheritdoc />
    public void SuccessfullyReceived(Message[] messages)
    {
        _actions.Add(new SuccessAllAction(_queue, messages));
    }

    /// <inheritdoc />
    public void MoveTo(string queueName, Message[] messages)
    {
        _actions.Add(new MoveAllAction(_queue, messages, queueName));
    }

    private interface IBatchAction
    {
        void Execute(IStoreTransaction transaction);
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

        public void Execute(IStoreTransaction transaction) =>
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

        public void Execute(IStoreTransaction transaction) =>
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

        public void Execute(IStoreTransaction transaction) =>
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

        public void Execute(IStoreTransaction transaction) =>
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

        public void Execute(IStoreTransaction transaction) { }

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

        public void Execute(IStoreTransaction transaction) { }

        public void Success()
        {
            foreach (var message in _messages)
                _queue.ReceiveLater(message, _time);
        }
    }
}
