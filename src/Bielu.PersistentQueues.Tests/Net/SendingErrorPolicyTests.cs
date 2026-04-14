using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Bielu.PersistentQueues.Logging;
using Bielu.PersistentQueues.Network;
using Bielu.PersistentQueues.Serialization;
using Bielu.PersistentQueues.Storage;
using Bielu.PersistentQueues.Storage.LMDB;
using Shouldly;
using Xunit;
using Xunit.Abstractions;

namespace Bielu.PersistentQueues.Tests.Net;

public class SendingErrorPolicyTests(ITestOutputHelper output) : TestBase
{
    Output = output;

    [Fact]
    public void max_attempts_is_reached()
    {
        ErrorPolicyScenario((policy, _, _) =>
        {
            var message = Message.Create(maxAttempts: 3).WithSentAttempts(3);
            policy.ShouldRetry(message).ShouldBeFalse();
        });
    }

    [Fact]
    public void max_attempts_is_not_reached()
    {
        ErrorPolicyScenario((policy, _, _) =>
        {
            var message = Message.Create(maxAttempts: 20).WithSentAttempts(5);
            policy.ShouldRetry(message).ShouldBeTrue();
        });
    }

    [Fact]
    public void deliver_by_has_expired()
    {
        ErrorPolicyScenario((policy, _, _) =>
        {
            var message = Message.Create(deliverBy: DateTime.Now.Subtract(TimeSpan.FromSeconds(1))).WithSentAttempts(5);
            policy.ShouldRetry(message).ShouldBeFalse();
        });
    }

    [Fact]
    public void deliver_by_has_not_expired()
    {
        ErrorPolicyScenario((policy, _, _) =>
        {
            var message = Message.Create(deliverBy: DateTime.Now.Add(TimeSpan.FromSeconds(1))).WithSentAttempts(5);
            policy.ShouldRetry(message).ShouldBeTrue();
        });
    }

    [Fact]
    public void has_neither_deliver_by_nor_max_attempts()
    {
        ErrorPolicyScenario((policy, _, _) =>
        {
            var message = NewMessage().WithSentAttempts(5);
            policy.ShouldRetry(message).ShouldBeTrue();
        });
    }

    [Fact]
    public Task message_is_observed_after_time()
    {
        return ErrorPolicyScenarioAsync(async (policy, store, failures, cancellation) =>
        {
            var message = Message.Create(
                destinationUri: "lq.tcp://localhost:5150/blah",
                maxAttempts: 2
            );
            using (var tx = store.BeginTransaction())
            {
                store.StoreOutgoing(tx, message);
                tx.Commit();
            }

            var errorTask = policy.StartRetriesAsync(cancellation.Token);
            var failure = new OutgoingMessageFailure
            {
                Messages = [message],
                ShouldRetry = true
            };
            var retryTask = policy.Retries.ReadAllAsync(cancellation.Token)
                .FirstAsync(cancellation.Token);
            failures.Writer.TryWrite(failure);
            var retryMessage = await retryTask;
            retryMessage.Id.ShouldBe(message.Id);
            await cancellation.CancelAsync();
            await DeterministicDelayAsync(50, CancellationToken.None);
            errorTask.IsCanceled.ShouldBeTrue();
        });
    }

    [Fact]
    public Task message_removed_from_storage_after_max()
    {
        return ErrorPolicyScenarioAsync(async (policy, store, failures, cancellation) =>
        {
            var message = Message.Create(
                destinationUri: "lq.tcp://localhost:5150/blah",
                maxAttempts: 1
            );
            using (var tx = store.BeginTransaction())
            {
                store.StoreOutgoing(tx, message);
                tx.Commit();
            }

            var failure = new OutgoingMessageFailure
            {
                Messages = [message]
            };
            var errorTask = policy.StartRetriesAsync(cancellation.Token);
            var retryTask = policy.Retries.ReadAllAsync(cancellation.Token).FirstAsync(cancellation.Token);
            failures.Writer.TryWrite(failure);
            await DeterministicDelayAsync(TimeSpan.FromSeconds(1), cancellation.Token);
            retryTask.IsCompleted.ShouldBeFalse();
            store.PersistedOutgoing().Any().ShouldBeFalse();
            await cancellation.CancelAsync();
            await DeterministicDelayAsync(50, CancellationToken.None);
            errorTask.IsCanceled.ShouldBeTrue();
        });
    }

    [Fact]
    public Task time_increases_with_each_failure()
    {
        return ErrorPolicyScenarioAsync(async (policy, store, failures, _) =>
        {
            using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(6));
            Message? observed = null;
            var message = Message.Create(
                destinationUri: "lq.tcp://localhost:5150/blah",
                maxAttempts: 5
            );
            using (var tx = store.BeginTransaction())
            {
                store.StoreOutgoing(tx, message);
                tx.Commit();
            }

            var errorTask = policy.StartRetriesAsync(cancellation.Token);
            var failure = new OutgoingMessageFailure
            {
                Messages = [message],
                ShouldRetry = true
            };
            var retriesTask = Task.Factory.StartNew(async () =>
            {
                await foreach (var msg in policy.Retries.ReadAllAsync(cancellation.Token))
                {
                    observed = msg;
                }
            }, cancellation.Token);
            failures.Writer.TryWrite(failure);
            await DeterministicDelayAsync(TimeSpan.FromSeconds(1.5), cancellation.Token);
            observed.ShouldNotBeNull("first");
            observed = null;
            failures.Writer.TryWrite(failure);
            observed.ShouldBeNull("second");
            await DeterministicDelayAsync(TimeSpan.FromSeconds(1), cancellation.Token);
            observed.ShouldBeNull("third");
            await Task.WhenAny(DeterministicDelayAsync(TimeSpan.FromSeconds(4), cancellation.Token));
            observed.ShouldNotBeNull("fourth");
            await cancellation.CancelAsync();
            await Task.WhenAny(errorTask.AsTask(), retriesTask);
        });
    }

    [Fact]
    public async Task errors_in_storage_dont_end_stream()
    {
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(1));
        var failures = Channel.CreateUnbounded<OutgoingMessageFailure>();
        var message = NewMessage();
        var store = new StubMessageStore();
        var errorPolicy = new SendingErrorPolicy(new RecordingLogger(OutputWriter), store, failures);
        var ended = false;
        var failure = new OutgoingMessageFailure
        {
            Messages = [message]
        };
        var retryTask = Task.Factory.StartNew(async () =>
        {
            await foreach (var _ in errorPolicy.Retries.ReadAllAsync(cancellation.Token))
            {
            }
            ended = true;
        }, cancellation.Token);
        failures.Writer.TryWrite(failure);
        await Task.WhenAny(retryTask, DeterministicDelayAsync(TimeSpan.FromSeconds(1), cancellation.Token));
        ended.ShouldBeFalse();
        await cancellation.CancelAsync();
    }

    private void ErrorPolicyScenario(Action<SendingErrorPolicy, IMessageStore, Channel<OutgoingMessageFailure>> scenario)
    {
        var logger = new RecordingLogger(OutputWriter);
        using var env = LightningEnvironment();
        using var store = new LmdbMessageStore(env, new MessageSerializer());
        var failures = Channel.CreateUnbounded<OutgoingMessageFailure>();
        var errorPolicy = new SendingErrorPolicy(logger, store, failures);
        scenario(errorPolicy, store, failures);
    }

    private async Task ErrorPolicyScenarioAsync(
        Func<SendingErrorPolicy, IMessageStore, Channel<OutgoingMessageFailure>, CancellationTokenSource, Task> scenario)
    {
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var logger = new RecordingLogger(OutputWriter);
        using var env = LightningEnvironment();
        using var store = new LmdbMessageStore(env, new MessageSerializer());
        var failures = Channel.CreateUnbounded<OutgoingMessageFailure>();
        var errorPolicy = new SendingErrorPolicy(logger, store, failures);
        await scenario(errorPolicy, store, failures, cancellation);
        await cancellation.CancelAsync();
    }
}

public class StubMessageStore : IMessageStore
{
    public void Dispose()
    {
    }

    public IStoreTransaction BeginTransaction()
    {
        throw new NotImplementedException();
    }

    public void CreateQueue(string queueName)
    {
        throw new NotImplementedException();
    }

    public void StoreIncoming(params IEnumerable<Message> messages)
    {
        throw new NotImplementedException();
    }

    public void StoreIncoming(IStoreTransaction transaction, params IEnumerable<Message> messages)
    {
        throw new NotImplementedException();
    }

    public void DeleteIncoming(params IEnumerable<Message> messages)
    {
        throw new NotImplementedException();
    }

    public IEnumerable<Message> PersistedIncoming(string queueName)
    {
        throw new NotImplementedException();
    }

    public IEnumerable<Message> PersistedOutgoing()
    {
        throw new NotImplementedException();
    }

    public void MoveToQueue(IStoreTransaction transaction, string queueName, Message message)
    {
        throw new NotImplementedException();
    }

    public void MoveToQueue(IStoreTransaction transaction, string queueName, IEnumerable<Message> messages)
    {
        throw new NotImplementedException();
    }

    public void SuccessfullyReceived(IStoreTransaction transaction, Message message)
    {
        throw new NotImplementedException();
    }

    public void SuccessfullyReceived(IStoreTransaction transaction, IEnumerable<Message> messages)
    {
        throw new NotImplementedException();
    }

    public void StoreOutgoing(IStoreTransaction tx, Message message)
    {
        throw new NotImplementedException();
    }

    public void StoreOutgoing(Message message)
    {
        throw new NotImplementedException();
    }

    public void StoreOutgoing(params IEnumerable<Message> messages)
    {
        throw new NotImplementedException();
    }

    public void StoreOutgoing(ReadOnlySpan<Message> messages)
    {
        throw new NotImplementedException();
    }

    public void FailedToSend(bool shouldRemove = false, params IEnumerable<Message> message)
    {
        throw new NotImplementedException();
    }

    public void SuccessfullySent(params IEnumerable<Message> messages)
    {
        throw new NotImplementedException();
    }

    public Message? GetMessage(string queueName, MessageId messageId)
    {
        throw new NotImplementedException();
    }

    public string[] GetAllQueues()
    {
        throw new NotImplementedException();
    }

    public void ClearAllStorage()
    {
        throw new NotImplementedException();
    }
}