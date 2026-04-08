using System;
using System.Collections.Generic;
using Bielu.PersistentQueues.Serialization;
using Bielu.PersistentQueues.Storage;

namespace Bielu.PersistentQueues;

internal class QueueContext : IQueueContext
{
    private readonly Queue _queue;
    private readonly Message _message;
    private readonly List<IQueueAction> _queueActions;
    private bool _messageDisposed;

    internal QueueContext(Queue queue, Message message)
    {
        _queue = queue;
        _message = message;
        _queueActions = new List<IQueueAction>();
    }

    public void CommitChanges()
    {
        using var transaction = _queue.Store.BeginTransaction();
        ExecuteActions(transaction);
        transaction.Commit();
        NotifySuccess();
    }

    internal void ExecuteActions(IStoreTransaction transaction)
    {
        foreach (var action in _queueActions)
        {
            action.Execute(transaction);
        }
    }

    internal void NotifySuccess()
    {
        foreach (var action in _queueActions)
        {
            action.Success();
        }
    }

    internal static void CommitBatch(IReadOnlyList<QueueContext> contexts)
    {
        if (contexts.Count == 0) return;

        using var transaction = contexts[0]._queue.Store.BeginTransaction();
        foreach (var context in contexts)
        {
            context.ExecuteActions(transaction);
        }
        transaction.Commit();

        foreach (var context in contexts)
        {
            context.NotifySuccess();
        }
    }

    public void Send(Message message)
    {
        _queueActions.Add(new SendAction(this, message));
    }

    public void ReceiveLater(TimeSpan timeSpan)
    {
        if (_messageDisposed)
            throw new InvalidOperationException("Cannot call ReceiveLater after SuccessfullyReceived or MoveTo has been called on this message.");
        _messageDisposed = true;
        var updatedMessage = _message.WithProcessingAttempts(_message.ProcessingAttempts + 1);
        if (_queue._deadLetterOptions.Enabled && ((_message.MaxAttempts.HasValue && updatedMessage.ProcessingAttempts >= _message.MaxAttempts.Value) || _queue._deadLetterOptions.GlobalMaxAttemptsForMessage <= updatedMessage.ProcessingAttempts))
        {
            var dlqName = DeadLetterConstants.QueueName;
            _queue.Store.CreateQueue(dlqName);
            _queueActions.Add(new DeadLetterAction(this, dlqName, updatedMessage, DeadLetterDiagnostics.Reasons.MaxProcessingAttempts));
            return;
        }
        _queueActions.Add(new ReceiveLaterTimeSpanAction(this, updatedMessage, timeSpan));
    }

    public void ReceiveLater(DateTimeOffset time)
    {
        if (_messageDisposed)
            throw new InvalidOperationException("Cannot call ReceiveLater after SuccessfullyReceived or MoveTo has been called on this message.");
        _messageDisposed = true;
        var updatedMessage = _message.WithProcessingAttempts(_message.ProcessingAttempts + 1);
        if (_queue._deadLetterOptions.Enabled && ((_message.MaxAttempts.HasValue && updatedMessage.ProcessingAttempts >= _message.MaxAttempts.Value) || _queue._deadLetterOptions.GlobalMaxAttemptsForMessage <= updatedMessage.ProcessingAttempts))
        {
            var dlqName = DeadLetterConstants.QueueName;
            _queue.Store.CreateQueue(dlqName);
            _queueActions.Add(new DeadLetterAction(this, dlqName, updatedMessage, DeadLetterDiagnostics.Reasons.MaxProcessingAttempts));
            return;
        }
        _queueActions.Add(new ReceiveLaterDateTimeOffsetAction(this, updatedMessage, time));
    }

    public void SuccessfullyReceived()
    {
        if (_messageDisposed)
            throw new InvalidOperationException("Cannot call SuccessfullyReceived after ReceiveLater or MoveTo has been called on this message.");
        _messageDisposed = true;
        _queueActions.Add(new SuccessAction(this));
    }

    public void MoveTo(string queueName)
    {
        if (_messageDisposed)
            throw new InvalidOperationException("Cannot call MoveTo after SuccessfullyReceived or ReceiveLater has been called on this message.");
        _messageDisposed = true;
        _queueActions.Add(new MoveAction(this, queueName));
    }

    public void MoveToDeadLetter()
    {
        if (!_queue._deadLetterOptions.Enabled)
            throw new InvalidOperationException("Dead letter queue is disabled. Enable it via WithDeadLetterQueue() in the queue configuration.");
        if (_messageDisposed)
            throw new InvalidOperationException("Cannot call MoveToDeadLetter after SuccessfullyReceived, ReceiveLater, or MoveTo has been called on this message.");
        _messageDisposed = true;
        var dlqName = DeadLetterConstants.QueueName;
        _queue.Store.CreateQueue(dlqName);
        _queueActions.Add(new DeadLetterAction(this, dlqName, _message, DeadLetterDiagnostics.Reasons.Manual));
    }

    public void Enqueue(Message message)
    {
        _queueActions.Add(new EnqueueAction(this, message));
    }

    /// <inheritdoc />
    public void Send<T>(
        T content,
        string? destinationUri = null,
        string? queueName = null,
        Dictionary<string, string>? headers = null,
        DateTime? deliverBy = null,
        int? maxAttempts = null,
        string? partitionKey = null)
    {
        var message = Message.Create(
            content,
            contentSerializer: _queue._contentSerializer,
            queue: queueName,
            destinationUri: destinationUri,
            deliverBy: deliverBy,
            maxAttempts: maxAttempts,
            headers: headers,
            partitionKey: partitionKey);
        Send(message);
    }

    /// <inheritdoc />
    public void Enqueue<T>(
        T content,
        string? queueName = null,
        Dictionary<string, string>? headers = null,
        string? partitionKey = null)
    {
        var message = Message.Create(
            content,
            contentSerializer: _queue._contentSerializer,
            queue: queueName,
            headers: headers,
            partitionKey: partitionKey);
        Enqueue(message);
    }

    private interface IQueueAction
    {
        void Execute(IStoreTransaction transaction);
        void Success();
    }

    private class SendAction : IQueueAction
    {
        private readonly QueueContext _context;
        private readonly Message _message;

        public SendAction(QueueContext context, Message message)
        {
            _context = context;
            _message = message;
        }

        public void Execute(IStoreTransaction transaction)
        {
            _context._queue.Store.StoreOutgoing(transaction, _message);
        }

        public void Success()
        {
        }
    }

    private class EnqueueAction : IQueueAction
    {
        private readonly QueueContext _context;
        private readonly Message _message;

        public EnqueueAction(QueueContext context, Message message)
        {
            _context = context;
            _message = message;
        }

        public void Execute(IStoreTransaction transaction)
        {
            _context._queue.Store.StoreIncoming(transaction, _message);
        }

        public void Success()
        {
        }
    }

    private class MoveAction : IQueueAction
    {
        private readonly QueueContext _context;
        private readonly string _queueName;

        public MoveAction(QueueContext context, string queueName)
        {
            _context = context;
            _queueName = queueName;
        }

        public void Execute(IStoreTransaction transaction)
        {
            _context._queue.Store.MoveToQueue(transaction, _queueName, _context._message);
        }

        public void Success()
        {
        }
    }

    private class SuccessAction : IQueueAction
    {
        private readonly QueueContext _context;

        public SuccessAction(QueueContext context)
        {
            _context = context;
        }

        public void Execute(IStoreTransaction transaction)
        {
            _context._queue.Store.SuccessfullyReceived(transaction, _context._message);
        }

        public void Success()
        {
        }
    }

    private class ReceiveLaterTimeSpanAction : IQueueAction
    {
        private readonly QueueContext _context;
        private readonly Message _updatedMessage;
        private readonly TimeSpan _timeSpan;

        public ReceiveLaterTimeSpanAction(QueueContext context, Message updatedMessage, TimeSpan timeSpan)
        {
            _context = context;
            _updatedMessage = updatedMessage;
            _timeSpan = timeSpan;
        }

        public void Execute(IStoreTransaction transaction)
        {
            // Remove the message from current queue before scheduling it for later
            _context._queue.Store.SuccessfullyReceived(transaction, _context._message);
        }

        public void Success()
        {
            _context._queue.ReceiveLater(_updatedMessage, _timeSpan);
        }
    }

    private class ReceiveLaterDateTimeOffsetAction : IQueueAction
    {
        private readonly QueueContext _context;
        private readonly Message _updatedMessage;
        private readonly DateTimeOffset _time;

        public ReceiveLaterDateTimeOffsetAction(QueueContext context, Message updatedMessage, DateTimeOffset time)
        {
            _context = context;
            _updatedMessage = updatedMessage;
            _time = time;
        }

        public void Execute(IStoreTransaction transaction)
        {
            // Remove the message from current queue before scheduling it for later
            _context._queue.Store.SuccessfullyReceived(transaction, _context._message);
        }

        public void Success()
        {
            _context._queue.ReceiveLater(_updatedMessage, _time);
        }
    }

    private class DeadLetterAction : IQueueAction
    {
        private readonly QueueContext _context;
        private readonly string _dlqName;
        private readonly Message _messageToStore;
        private readonly string _reason;

        public DeadLetterAction(QueueContext context, string dlqName, Message messageToStore, string reason)
        {
            _context = context;
            _dlqName = dlqName;
            _messageToStore = messageToStore;
            _reason = reason;
        }

        public void Execute(IStoreTransaction transaction)
        {
            var messageWithOrigin = _messageToStore.WithOriginalQueue(
                _context._message.QueueString ?? "unknown");
            _context._queue.Store.MoveToQueue(transaction, _dlqName, messageWithOrigin);
        }

        public void Success()
        {
            DeadLetterDiagnostics.RecordMessageDeadLettered(
                _context._message.QueueString ?? "unknown", _reason);
        }
    }
}