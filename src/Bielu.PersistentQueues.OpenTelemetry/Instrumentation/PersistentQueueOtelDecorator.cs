using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Net;
using System.Runtime.CompilerServices;
using Bielu.PersistentQueues.OpenTelemetry.Instrumentation.Metrics;
using Bielu.PersistentQueues.OpenTelemetry.Instrumentation.Tracing;
using Bielu.PersistentQueues.Storage;

namespace Bielu.PersistentQueues.OpenTelemetry.Instrumentation;

public class PersistentQueueOtelDecorator : IQueue
{
    private readonly IQueue _queue;
    private readonly QueueMetrics _metrics;
    private readonly QueueActivitySource _activitySource;
    private readonly ObservableGauge<int> _activeQueuesGauge;
    private readonly ObservableGauge<long>? _storageUsedBytesGauge;
    private readonly ObservableGauge<long>? _storageTotalBytesGauge;
    private readonly ObservableGauge<double>? _storageUsagePercentGauge;
    private readonly ObservableGauge<long> _deadLetterQueueDepthGauge;

    public PersistentQueueOtelDecorator(IQueue queue, QueueMetrics queueMetrics, QueueActivitySource activitySource)
    {
        _queue = queue;
        _metrics = queueMetrics;
        _activitySource = activitySource;
        _activeQueuesGauge = _metrics.CreateActiveQueuesGauge(() => _queue.Queues.Length);

        // Register storage usage gauges if the store supports usage reporting
        var store = _queue.Store;
        if (store.GetStorageUsageInfo() != null)
        {
            _storageUsedBytesGauge = _metrics.CreateStorageUsedBytesGauge(
                () => store.GetStorageUsageInfo()?.UsedBytes ?? 0);
            _storageTotalBytesGauge = _metrics.CreateStorageTotalBytesGauge(
                () => store.GetStorageUsageInfo()?.TotalBytes ?? 0);
            _storageUsagePercentGauge = _metrics.CreateStorageUsagePercentGauge(
                () => store.GetStorageUsageInfo()?.UsagePercentage ?? 0);
        }

        // Register dead letter queue depth gauge — reports message count in the shared DLQ
        _deadLetterQueueDepthGauge = _metrics.CreateDeadLetterQueueDepthGauge(() =>
        {
            var measurements = new List<Measurement<long>>();
            foreach (var queueName in store.GetAllQueues())
            {
                if (!DeadLetterConstants.IsDeadLetterQueue(queueName))
                    continue;
                var depth = store.PersistedIncoming(queueName).Count();
                measurements.Add(new Measurement<long>(depth,
                    new KeyValuePair<string, object?>("queue.name", queueName)));
            }
            return measurements;
        });
    }

    public void Dispose()
    {
        _queue.Dispose();
    }

    public async ValueTask DisposeAsync()
    {
        await _queue.DisposeAsync();
    }

    public void CreateQueue(string queueName)
    {
        using var activity = _activitySource.StartActivity(ActivityNames.CreateQueue, ActivityKind.Internal);
        QueueActivitySource.SetQueueTags(activity, queueName);

        try
        {
            _queue.CreateQueue(queueName);
        }
        catch (Exception ex)
        {
            _metrics.RecordOperationError("CreateQueue", queueName);
            _activitySource.RecordException(activity, ex);
            throw;
        }
    }

    public void Start()
    {
        using var activity = _activitySource.StartActivity(ActivityNames.Start, ActivityKind.Internal);

        try
        {
            _queue.Start();
        }
        catch (Exception ex)
        {
            _metrics.RecordOperationError("Start");
            _activitySource.RecordException(activity, ex);
            throw;
        }
    }

    public async IAsyncEnumerable<IMessageContext> Receive(string queueName, int pollIntervalInMilliseconds = 200,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        using var activity = _activitySource.StartActivity(ActivityNames.Receive, ActivityKind.Consumer);
        QueueActivitySource.SetQueueTags(activity, queueName);
        QueueActivitySource.SetPollIntervalTag(activity, pollIntervalInMilliseconds);

        await foreach (var messageContext in _queue.Receive(queueName, pollIntervalInMilliseconds, cancellationToken))
        {
            var dequeueStartTime = Stopwatch.GetTimestamp();

            _metrics.RecordMessagesReceived(1, queueName);

            var dequeueElapsed = Stopwatch.GetElapsedTime(dequeueStartTime).TotalMilliseconds;
            _metrics.RecordDequeueDuration(dequeueElapsed, queueName);

            var processingStartTime = Stopwatch.GetTimestamp();

            using var messageActivity =
                _activitySource.StartActivity(ActivityNames.ProcessMessage, ActivityKind.Consumer);
            QueueActivitySource.SetMessageTags(messageActivity, messageContext.Message.Id.MessageIdentifier, queueName);

            yield return messageContext;

            var processingElapsed = Stopwatch.GetElapsedTime(processingStartTime).TotalMilliseconds;
            _metrics.RecordProcessingDuration(processingElapsed, queueName);
        }
    }

    public async IAsyncEnumerable<IBatchQueueContext> ReceiveBatch(string queueName, int maxMessages = 0,
        int batchTimeoutInMilliseconds = 0,
        int pollIntervalInMilliseconds = 200, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        using var activity = _activitySource.StartActivity(ActivityNames.ReceiveBatch, ActivityKind.Consumer);
        QueueActivitySource.SetQueueTags(activity, queueName);
        QueueActivitySource.SetPollIntervalTag(activity, pollIntervalInMilliseconds);
        activity?.SetTag("max.messages", maxMessages);
        activity?.SetTag("batch.timeout", batchTimeoutInMilliseconds);

        await foreach (var messageContext in _queue.ReceiveBatch(queueName, maxMessages, batchTimeoutInMilliseconds,
                           pollIntervalInMilliseconds, cancellationToken))
        {
            var dequeueStartTime = Stopwatch.GetTimestamp();
            var batchSize = messageContext.Messages.Length;

            _metrics.RecordMessagesReceived(batchSize, queueName);
            _metrics.RecordBatchSize(batchSize, queueName);
            _metrics.RecordBatchProcessed(queueName);
            var dequeueElapsed = Stopwatch.GetElapsedTime(dequeueStartTime).TotalMilliseconds;
            _metrics.RecordDequeueDuration(dequeueElapsed, queueName, batchSize);

            var processingStartTime = Stopwatch.GetTimestamp();

            using var messageActivity =
                _activitySource.StartActivity(ActivityNames.ProcessBatch, ActivityKind.Consumer);
            QueueActivitySource.SetBatchTags(messageActivity, batchSize, queueName);
            messageActivity?.SetTag("message.Id.MessageIdentifiers",
                string.Join(",", messageContext.Messages.Select(x => x.Id)));

            yield return messageContext;

            var processingElapsed = Stopwatch.GetElapsedTime(processingStartTime).TotalMilliseconds;
            _metrics.RecordProcessingDuration(processingElapsed, queueName, batchSize);
        }
    }

    public void ReceiveLater(Message message, TimeSpan timeSpan)
    {
        using var activity = _activitySource.StartActivity(ActivityNames.ReceiveLater, ActivityKind.Internal);
        QueueActivitySource.SetMessageTags(activity, message.Id.MessageIdentifier, message.Queue.ToString());
        QueueActivitySource.SetDelayTags(activity, timeSpan);

        try
        {
            _queue.ReceiveLater(message, timeSpan);
        }
        catch (Exception ex)
        {
            _metrics.RecordOperationError("ReceiveLater", message.Queue.ToString().ToString());
            _activitySource.RecordException(activity, ex);
            throw;
        }
    }

    public void ReceiveLater(Message message, DateTimeOffset time)
    {
        using var activity = _activitySource.StartActivity(ActivityNames.ReceiveLater, ActivityKind.Internal);
        QueueActivitySource.SetMessageTags(activity, message.Id.MessageIdentifier, message.Queue.ToString());
        QueueActivitySource.SetScheduledTimeTags(activity, time);

        try
        {
            _queue.ReceiveLater(message, time);
        }
        catch (Exception ex)
        {
            _metrics.RecordOperationError("ReceiveLater", message.Queue.ToString());
            _activitySource.RecordException(activity, ex);
            throw;
        }
    }

    public void MoveToQueue(string queueName, Message message)
    {
        using var activity = _activitySource.StartActivity(ActivityNames.MoveToQueue, ActivityKind.Internal);
        QueueActivitySource.SetQueueTags(activity, queueName);
        QueueActivitySource.SetMessageTags(activity, message.Id.MessageIdentifier);
        activity?.SetTag("source.queue", message.Queue.ToString());

        try
        {
            _queue.MoveToQueue(queueName, message);
        }
        catch (Exception ex)
        {
            _metrics.RecordOperationError("MoveToQueue", queueName);
            _activitySource.RecordException(activity, ex);
            throw;
        }
    }

    public void Send(params Message[] messages)
    {
        using var activity = _activitySource.StartActivity(ActivityNames.SendBatch, ActivityKind.Producer);
        QueueActivitySource.SetBatchTags(activity, messages.Length);
        activity?.SetTag("message.count", messages.Length);

        try
        {
            var startTime = Stopwatch.GetTimestamp();

            _queue.Send(messages);

            var elapsed = Stopwatch.GetElapsedTime(startTime).TotalMilliseconds;
            _metrics.RecordMessagesSent(messages.Length);
            _metrics.RecordBatchSize(messages.Length);

            // Record enqueue duration for batch send (uses first message's queue name if available)
            if (messages.Length > 0)
            {
                _metrics.RecordEnqueueDuration(elapsed, messages[0].Queue.ToString(), messages.Length);
            }
        }
        catch (Exception ex)
        {
            _metrics.RecordOperationError("Send", batchSize: messages.Length);
            _activitySource.RecordException(activity, ex);
            throw;
        }
    }

    public void Send(Message message)
    {
        using var activity = _activitySource.StartActivity(ActivityNames.Send, ActivityKind.Producer);
        QueueActivitySource.SetMessageTags(activity, message.Id.MessageIdentifier, message.Queue.ToString(),
            message.Destination?.ToString());

        try
        {
            var startTime = Stopwatch.GetTimestamp();

            _queue.Send(message);

            var elapsed = Stopwatch.GetElapsedTime(startTime).TotalMilliseconds;
            _metrics.RecordMessageSent(message.Queue.ToString());
            _metrics.RecordEnqueueDuration(elapsed, message.Queue.ToString());
        }
        catch (Exception ex)
        {
            _metrics.RecordOperationError("Send", message.Queue.ToString());
            _activitySource.RecordException(activity, ex);
            throw;
        }
    }

    public void Enqueue(Message message)
    {
        using var activity = _activitySource.StartActivity(ActivityNames.Enqueue, ActivityKind.Producer);
        QueueActivitySource.SetMessageTags(activity, message.Id.MessageIdentifier, message.Queue.ToString());

        try
        {
            var startTime = Stopwatch.GetTimestamp();

            _queue.Enqueue(message);

            var elapsed = Stopwatch.GetElapsedTime(startTime).TotalMilliseconds;
            _metrics.RecordMessageEnqueued(message.Queue.ToString());
            _metrics.RecordEnqueueDuration(elapsed, message.Queue.ToString());
        }
        catch (Exception ex)
        {
            _metrics.RecordOperationError("Enqueue", message.Queue.ToString());
            _activitySource.RecordException(activity, ex);
            throw;
        }
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
        => _queue.Send(content, destinationUri, queueName, headers, deliverBy, maxAttempts, partitionKey);

    /// <inheritdoc />
    public void Enqueue<T>(
        T content,
        string? queueName = null,
        Dictionary<string, string>? headers = null,
        string? partitionKey = null)
        => _queue.Enqueue(content, queueName, headers, partitionKey);

    public IMessageStore Store => _queue.Store;
    public string[] Queues => _queue.Queues;
    public IPEndPoint Endpoint => _queue.Endpoint;

    /// <inheritdoc />
    public int RequeueDeadLetterMessages()
        => _queue.RequeueDeadLetterMessages();

    /// <inheritdoc />
    public int ClearDeadLetterQueue()
        => _queue.ClearDeadLetterQueue();
}