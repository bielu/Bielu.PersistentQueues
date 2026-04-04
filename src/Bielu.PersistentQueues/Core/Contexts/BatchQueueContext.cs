using System;
using System.Collections.Generic;
using System.Linq;
using Bielu.PersistentQueues.Serialization;
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
    private readonly HashSet<Guid> _disposedMessageIds;

    internal BatchQueueContext(Message[] messages, Queue queue)
    {
        Messages = messages;
        _queue = queue;
        _actions = new List<IBatchAction>();
        _disposedMessageIds = new HashSet<Guid>();
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

    /// <inheritdoc />
    public void ReceiveLater(TimeSpan timeSpan)
    {
        ValidateAndMarkMessages(Messages, "ReceiveLater");
        var updatedMessages = UpdateProcessingAttempts(Messages);
        var (dlqMessages, retryMessages) = SplitByMaxAttempts(updatedMessages, _queue._deadLetterOptions.Enabled);
        if (dlqMessages.Length > 0)
        {
            EnsureDeadLetterQueues(dlqMessages);
            _actions.Add(new DeadLetterAllAction(_queue, dlqMessages, DeadLetterDiagnostics.Reasons.MaxProcessingAttempts));
        }
        if (retryMessages.Length > 0)
            _actions.Add(new ReceiveLaterTimeSpanAction(_queue, retryMessages, timeSpan));
    }

    /// <inheritdoc />
    public void ReceiveLater(DateTimeOffset time)
    {
        ValidateAndMarkMessages(Messages, "ReceiveLater");
        var updatedMessages = UpdateProcessingAttempts(Messages);
        var (dlqMessages, retryMessages) = SplitByMaxAttempts(updatedMessages, _queue._deadLetterOptions.Enabled);
        if (dlqMessages.Length > 0)
        {
            EnsureDeadLetterQueues(dlqMessages);
            _actions.Add(new DeadLetterAllAction(_queue, dlqMessages, DeadLetterDiagnostics.Reasons.MaxProcessingAttempts));
        }
        if (retryMessages.Length > 0)
            _actions.Add(new ReceiveLaterDateTimeOffsetAction(_queue, retryMessages, time));
    }

    /// <inheritdoc />
    public void SuccessfullyReceived()
    {
        ValidateAndMarkMessages(Messages, "SuccessfullyReceived");
        _actions.Add(new SuccessAllAction(_queue, Messages));
    }

    /// <inheritdoc />
    public void MoveTo(string queueName)
    {
        ValidateAndMarkMessages(Messages, "MoveTo");
        _actions.Add(new MoveAllAction(_queue, Messages, queueName));
    }

    /// <inheritdoc />
    public void ReceiveLater(Message[] messages, TimeSpan timeSpan)
    {
        ValidateAndMarkMessages(messages, "ReceiveLater");
        var updatedMessages = UpdateProcessingAttempts(messages);
        var (dlqMessages, retryMessages) = SplitByMaxAttempts(updatedMessages, _queue._deadLetterOptions.Enabled);
        if (dlqMessages.Length > 0)
        {
            EnsureDeadLetterQueues(dlqMessages);
            _actions.Add(new DeadLetterAllAction(_queue, dlqMessages, DeadLetterDiagnostics.Reasons.MaxProcessingAttempts));
        }
        if (retryMessages.Length > 0)
            _actions.Add(new ReceiveLaterTimeSpanAction(_queue, retryMessages, timeSpan));
    }

    public void ReceiveLater(Guid[] messageIds, TimeSpan timeSpan)
    {
        var messages = Messages.Where(x => messageIds.Contains(x.Id.MessageIdentifier)).ToArray();
        ValidateAndMarkMessages(messages, "ReceiveLater");
        var updatedMessages = UpdateProcessingAttempts(messages);
        var (dlqMessages, retryMessages) = SplitByMaxAttempts(updatedMessages, _queue._deadLetterOptions.Enabled);
        if (dlqMessages.Length > 0)
        {
            EnsureDeadLetterQueues(dlqMessages);
            _actions.Add(new DeadLetterAllAction(_queue, dlqMessages, DeadLetterDiagnostics.Reasons.MaxProcessingAttempts));
        }
        if (retryMessages.Length > 0)
            _actions.Add(new ReceiveLaterTimeSpanAction(_queue, retryMessages, timeSpan));
    }

    /// <inheritdoc />
    public void ReceiveLater(Message[] messages, DateTimeOffset time)
    {
        ValidateAndMarkMessages(messages, "ReceiveLater");
        var updatedMessages = UpdateProcessingAttempts(messages);
        var (dlqMessages, retryMessages) = SplitByMaxAttempts(updatedMessages, _queue._deadLetterOptions.Enabled);
        if (dlqMessages.Length > 0)
        {
            EnsureDeadLetterQueues(dlqMessages);
            _actions.Add(new DeadLetterAllAction(_queue, dlqMessages, DeadLetterDiagnostics.Reasons.MaxProcessingAttempts));
        }
        if (retryMessages.Length > 0)
            _actions.Add(new ReceiveLaterDateTimeOffsetAction(_queue, retryMessages, time));
    }

    public void ReceiveLater(Guid[] messageIds, DateTimeOffset time)
    {
        var messages = Messages.Where(x => messageIds.Contains(x.Id.MessageIdentifier)).ToArray();
        ValidateAndMarkMessages(messages, "ReceiveLater");
        var updatedMessages = UpdateProcessingAttempts(messages);
        var (dlqMessages, retryMessages) = SplitByMaxAttempts(updatedMessages, _queue._deadLetterOptions.Enabled);
        if (dlqMessages.Length > 0)
        {
            EnsureDeadLetterQueues(dlqMessages);
            _actions.Add(new DeadLetterAllAction(_queue, dlqMessages, DeadLetterDiagnostics.Reasons.MaxProcessingAttempts));
        }
        if (retryMessages.Length > 0)
            _actions.Add(new ReceiveLaterDateTimeOffsetAction(_queue, retryMessages, time));
    }

    /// <inheritdoc />
    public void SuccessfullyReceived(Message[] messages)
    {
        ValidateAndMarkMessages(messages, "SuccessfullyReceived");
        _actions.Add(new SuccessAllAction(_queue, messages));
    }
    /// <inheritdoc />
    public void SuccessfullyReceived(Guid[] messageIds)
    {
        var messages = Messages.Where(x => messageIds.Contains(x.Id.MessageIdentifier)).ToArray();
        ValidateAndMarkMessages(messages, "SuccessfullyReceived");
        _actions.Add(new SuccessAllAction(_queue, messages));
    }
    /// <inheritdoc />
    public void MoveTo(string queueName, Message[] messages)
    {
        ValidateAndMarkMessages(messages, "MoveTo");
        _actions.Add(new MoveAllAction(_queue, messages, queueName));
    }

    /// <inheritdoc />
    public void MoveToDeadLetter()
    {
        ValidateAndMarkMessages(Messages, "MoveToDeadLetter");
        EnsureDeadLetterQueues(Messages);
        _actions.Add(new DeadLetterAllAction(_queue, Messages, DeadLetterDiagnostics.Reasons.Manual));
    }

    /// <inheritdoc />
    public void MoveToDeadLetter(Message[] messages)
    {
        ValidateAndMarkMessages(messages, "MoveToDeadLetter");
        EnsureDeadLetterQueues(messages);
        _actions.Add(new DeadLetterAllAction(_queue, messages, DeadLetterDiagnostics.Reasons.Manual));
    }

    /// <inheritdoc />
    public void MoveToDeadLetter(Guid[] messageIds)
    {
        var messages = Messages.Where(x => messageIds.Contains(x.Id.MessageIdentifier)).ToArray();
        ValidateAndMarkMessages(messages, "MoveToDeadLetter");
        EnsureDeadLetterQueues(messages);
        _actions.Add(new DeadLetterAllAction(_queue, messages, DeadLetterDiagnostics.Reasons.Manual));
    }

    private void ValidateAndMarkMessages(Message[] messages, string operationName)
    {
        foreach (var message in messages)
        {
            if (!_disposedMessageIds.Add(message.Id.MessageIdentifier))
            {
                throw new InvalidOperationException($"Cannot call {operationName} on message {message.Id.MessageIdentifier} - it has already been processed by another operation (SuccessfullyReceived, MoveTo, or ReceiveLater).");
            }
        }
    }

    private static Message[] UpdateProcessingAttempts(Message[] messages)
    {
        var updated = new Message[messages.Length];
        for (var i = 0; i < messages.Length; i++)
            updated[i] = messages[i].WithProcessingAttempts(messages[i].ProcessingAttempts + 1);
        return updated;
    }

    private static (Message[] dlq, Message[] retry) SplitByMaxAttempts(Message[] updatedMessages, bool dlqEnabled)
    {
        if (!dlqEnabled)
            return ([], updatedMessages);
        var dlq = updatedMessages.Where(m => m.MaxAttempts.HasValue && m.ProcessingAttempts >= m.MaxAttempts.Value).ToArray();
        var retry = updatedMessages.Where(m => !m.MaxAttempts.HasValue || m.ProcessingAttempts < m.MaxAttempts.Value).ToArray();
        return (dlq, retry);
    }

    private void EnsureDeadLetterQueues(Message[] messages)
    {
        foreach (var message in messages)
        {
            var dlqName = DeadLetterConstants.GetDeadLetterQueueName(message.QueueString ?? "unknown");
            _queue.Store.CreateQueue(dlqName);
        }
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

        public void Execute(IStoreTransaction transaction)
        {
            // Remove the messages from current queue before scheduling them for later.
            // IDs are unchanged by WithProcessingAttempts, so deletion is correct.
            _queue.Store.SuccessfullyReceived(transaction, _messages);
        }

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

        public void Execute(IStoreTransaction transaction)
        {
            // Remove the messages from current queue before scheduling them for later.
            // IDs are unchanged by WithProcessingAttempts, so deletion is correct.
            _queue.Store.SuccessfullyReceived(transaction, _messages);
        }

        public void Success()
        {
            foreach (var message in _messages)
                _queue.ReceiveLater(message, _time);
        }
    }

    private class DeadLetterAllAction : IBatchAction
    {
        private readonly Queue _queue;
        private readonly Message[] _messages;
        private readonly string _reason;

        public DeadLetterAllAction(Queue queue, Message[] messages, string reason)
        {
            _queue = queue;
            _messages = messages;
            _reason = reason;
        }

        public void Execute(IStoreTransaction transaction)
        {
            foreach (var message in _messages)
            {
                var sourceQueue = message.QueueString ?? "unknown";
                var dlqName = DeadLetterConstants.GetDeadLetterQueueName(sourceQueue);
                var messageWithOrigin = message.WithOriginalQueue(sourceQueue);
                _queue.Store.MoveToQueue(transaction, dlqName, messageWithOrigin);
            }
        }

        public void Success()
        {
            foreach (var message in _messages)
            {
                DeadLetterDiagnostics.RecordMessageDeadLettered(
                    message.QueueString ?? "unknown", _reason);
            }
        }
    }
}
