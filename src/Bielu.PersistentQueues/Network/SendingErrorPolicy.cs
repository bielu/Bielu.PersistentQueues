using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Bielu.PersistentQueues.Storage;

namespace Bielu.PersistentQueues.Network;

public class SendingErrorPolicy
{
    private readonly ILogger _logger;
    private readonly IMessageStore _store;
    private readonly Channel<OutgoingMessageFailure> _failedToConnect;
    private readonly Channel<Message> _retries;
    private readonly DeadLetterOptions _deadLetterOptions;

#pragma warning disable BIELU004 // ILogger is used intentionally to support both DI (ILogger<T>) and builder (ILogger) patterns
    public SendingErrorPolicy(ILogger logger, IMessageStore store, Channel<OutgoingMessageFailure> failedToConnect, DeadLetterOptions? deadLetterOptions = null)
#pragma warning restore BIELU004
    {
        _logger = logger;
        _store = store;
        _failedToConnect = failedToConnect;
        _retries = Channel.CreateUnbounded<Message>();
        _deadLetterOptions = deadLetterOptions ?? new DeadLetterOptions();
    }

    public ChannelReader<Message> Retries => _retries.Reader;

    public async ValueTask StartRetriesAsync(CancellationToken cancellationToken)
    {
        await foreach (var messageFailure in _failedToConnect.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
        {
            if (cancellationToken.IsCancellationRequested)
                break;
            var incrementedMessages = IncrementSentAttempt(messageFailure.Messages).ToList();
            IncrementAttemptAndStoreForRecovery(!messageFailure.ShouldRetry, incrementedMessages);
            await HandleMessageRetriesAsync(messageFailure.ShouldRetry, cancellationToken, incrementedMessages).ConfigureAwait(false);
        }
    }

    private async Task HandleMessageRetriesAsync(bool shouldRetry, CancellationToken cancellationToken, params IEnumerable<Message> messages)
    {
        foreach (var message in messages)
        {
            if (!ShouldRetry(message, shouldRetry))
            {
                if (_deadLetterOptions.Enabled)
                    MoveToDeadLetter(message);
                continue;
            }
            await Task.Delay(TimeSpan.FromSeconds(message.SentAttempts * message.SentAttempts), cancellationToken).ConfigureAwait(false);
            await _retries.Writer.WriteAsync(message, cancellationToken).ConfigureAwait(false);
        }
    }

    public bool ShouldRetry(Message message, bool shouldRetryOverride = true)
    {
        if (!shouldRetryOverride)
            return false;
        var totalAttempts = message.MaxAttempts ?? 100;
        _logger.PolicyShouldRetryAttempts(message.SentAttempts, totalAttempts);
        if(message.DeliverBy.HasValue)
            _logger.PolicyShouldRetryTiming(message.DeliverBy, DateTime.Now);
        return message.SentAttempts < totalAttempts
               &&
               (!message.DeliverBy.HasValue || DateTime.Now < message.DeliverBy);
    }

    private void IncrementAttemptAndStoreForRecovery(bool shouldRemove, params IEnumerable<Message> messages)
    {
        try
        {

            _store.FailedToSend(shouldRemove, messages);
        }
        catch (Exception ex)
        {
            _logger.PolicyIncrementFailureError(ex);
        }
    }

    private static IEnumerable<Message> IncrementSentAttempt(IEnumerable<Message> messages)
    {
        foreach (var message in messages)
        {
            yield return message.WithSentAttempts(message.SentAttempts + 1);
        }
    }

    private void MoveToDeadLetter(Message message)
    {
        var sourceQueue = message.QueueString ?? "unknown";
        var dlqName = DeadLetterConstants.QueueName;
        try
        {
            _store.CreateQueue(dlqName);
            _store.StoreIncoming(message.WithOriginalQueue(sourceQueue).WithQueue(dlqName));
            DeadLetterDiagnostics.RecordMessageDeadLettered(sourceQueue, DeadLetterDiagnostics.Reasons.SendFailed);
        }
        catch (Exception ex)
        {
            _logger.PolicyIncrementFailureError(ex);
        }
    }
}