using System;
using System.Collections.Generic;
using Bielu.PersistentQueues.Serialization;
using Bielu.PersistentQueues.Storage;

namespace Bielu.PersistentQueues;

#pragma warning disable BIELU010 // QueueContext is not a wrapper - it provides a context API around a Queue instance
internal class QueueContext : IQueueContext
#pragma warning restore BIELU010
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

    private class SendAction(QueueContext context, Message message) : IQueueAction
    {
        public void Execute(IStoreTransaction transaction)
        {
            context._queue.Store.StoreOutgoing(transaction, message);
        }

        public void Success()
        {
        }
    }

    private class EnqueueAction(QueueContext context, Message message) : IQueueAction
    {
        public void Execute(IStoreTransaction transaction)
        {
            context._queue.Store.StoreIncoming(transaction, message);
        }

        public void Success()
        {
        }
    }

    private class MoveAction(QueueContext context, string queueName) : IQueueAction
    {
        public void Execute(IStoreTransaction transaction)
        {
            context._queue.Store.MoveToQueue(transaction, queueName, context._message);
        }

        public void Success()
        {
        }
    }

    private class SuccessAction(QueueContext context) : IQueueAction
    {
        public void Execute(IStoreTransaction transaction)
        {
            context._queue.Store.SuccessfullyReceived(transaction, context._message);
        }

        public void Success()
        {
        }
    }

    private class ReceiveLaterTimeSpanAction(QueueContext context, Message updatedMessage, TimeSpan timeSpan) : IQueueAction
    {
        public void Execute(IStoreTransaction transaction)
        {
            // Remove the message from current queue before scheduling it for later
            context._queue.Store.SuccessfullyReceived(transaction, context._message);
        }

        public void Success()
        {
            context._queue.ReceiveLater(updatedMessage, timeSpan);
        }
    }

    private class ReceiveLaterDateTimeOffsetAction(QueueContext context, Message updatedMessage, DateTimeOffset time) : IQueueAction
    {
        public void Execute(IStoreTransaction transaction)
        {
            // Remove the message from current queue before scheduling it for later
            context._queue.Store.SuccessfullyReceived(transaction, context._message);
        }

        public void Success()
        {
            context._queue.ReceiveLater(updatedMessage, time);
        }
    }

    private class DeadLetterAction(QueueContext context, string dlqName, Message messageToStore, string reason) : IQueueAction
    {
        public void Execute(IStoreTransaction transaction)
        {
            var messageWithOrigin = messageToStore.WithOriginalQueue(
                context._message.QueueString ?? "unknown");
            context._queue.Store.MoveToQueue(transaction, dlqName, messageWithOrigin);
        }

        public void Success()
        {
            DeadLetterDiagnostics.RecordMessageDeadLettered(
                context._message.QueueString ?? "unknown", reason);
        }
    }
}