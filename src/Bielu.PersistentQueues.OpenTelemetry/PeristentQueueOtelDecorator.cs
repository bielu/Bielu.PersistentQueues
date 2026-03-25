using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Net;
using System.Runtime.CompilerServices;
using Bielu.PersistentQueues.Storage;
using OpenTelemetry.Trace;

namespace Bielu.PersistentQueues.OpenTelemetry;

public class PersistentQueueOtelDecorator : IQueue
{
    private readonly IQueue _queue;
    private readonly ObservableGauge<int> _activeQueuesGauge;

    private static readonly ActivitySource ActivitySource = new(QueueExtensions.ActivityName, "1.0.0");
    private static readonly Meter Meter = new(QueueExtensions.MetricName, "1.0.0");

    private static readonly Counter<long> MessagesSentCounter = Meter.CreateCounter<long>(
        "bielupersistentqueues.messages.sent",
        description: "Total number of messages sent");

    private static readonly Counter<long> MessagesReceivedCounter = Meter.CreateCounter<long>(
        "bielupersistentqueues.messages.received",
        description: "Total number of messages received");

    private static readonly Counter<long> MessagesEnqueuedCounter = Meter.CreateCounter<long>(
        "bielupersistentqueues.messages.enqueued",
        description: "Total number of messages enqueued");

    private static readonly Counter<long> OperationErrorsCounter = Meter.CreateCounter<long>(
        "bielupersistentqueues.operations.errors",
        description: "Total number of operation errors");

    private static readonly Histogram<double> MessageProcessingDuration = Meter.CreateHistogram<double>(
        "bielupersistentqueues.message.processing.duration",
        unit: "ms",
        description: "Duration of message processing");

    private static readonly Histogram<int> BatchSizeHistogram = Meter.CreateHistogram<int>(
        "bielupersistentqueues.batch.size",
        description: "Number of messages in a batch");

    public PersistentQueueOtelDecorator(IQueue queue)
    {
        _queue = queue;
        _activeQueuesGauge = Meter.CreateObservableGauge(
            "bielupersistentqueues.queues.active",
            () => new Measurement<int>(_queue.Queues.Length),
            description: "Number of active queues");
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
        using var activity = ActivitySource.StartActivity("CreateQueue", ActivityKind.Internal);
        activity?.SetTag("queue.name", queueName);

        try
        {
            _queue.CreateQueue(queueName);
        }
        catch (Exception ex)
        {
            OperationErrorsCounter.Add(1,
                new KeyValuePair<string, object?>("operation", "CreateQueue"),
                new KeyValuePair<string, object?>("queue.name", queueName));
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            activity?.AddException(ex);
            throw;
        }
    }

    public void Start()
    {
        using var activity = ActivitySource.StartActivity("Start", ActivityKind.Internal);

        try
        {
            _queue.Start();
        }
        catch (Exception ex)
        {
            OperationErrorsCounter.Add(1, new KeyValuePair<string, object?>("operation", "Start"));
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            activity?.AddException(ex);
            throw;
        }
    }

    public async IAsyncEnumerable<IMessageContext> Receive(string queueName, int pollIntervalInMilliseconds = 200,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        using var activity = ActivitySource.StartActivity("Receive", ActivityKind.Consumer);
        activity?.SetTag("queue.name", queueName);
        activity?.SetTag("poll.interval", pollIntervalInMilliseconds);

        await foreach (var messageContext in _queue.Receive(queueName, pollIntervalInMilliseconds, cancellationToken))
        {
            var startTime = Stopwatch.GetTimestamp();

            MessagesReceivedCounter.Add(1, new KeyValuePair<string, object?>("queue.name", queueName));

            using var messageActivity = ActivitySource.StartActivity("ProcessMessage", ActivityKind.Consumer);
            messageActivity?.SetTag("queue.name", queueName);
            messageActivity?.SetTag("message.id", messageContext.Message.Id);

            yield return messageContext;

            var elapsed = Stopwatch.GetElapsedTime(startTime).TotalMilliseconds;
            MessageProcessingDuration.Record(elapsed, new KeyValuePair<string, object?>("queue.name", queueName));
        }
    }

    public async IAsyncEnumerable<IBatchQueueContext> ReceiveBatch(string queueName, int maxMessages = 0,
        int batchTimeoutInMilliseconds = 0,
        int pollIntervalInMilliseconds = 200, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        using var activity = ActivitySource.StartActivity("ReceiveBatch", ActivityKind.Consumer);
        activity?.SetTag("queue.name", queueName);
        activity?.SetTag("poll.interval", pollIntervalInMilliseconds);
        activity?.SetTag("max.messages", maxMessages);
        activity?.SetTag("batch.timeout", batchTimeoutInMilliseconds);

        await foreach (var messageContext in _queue.ReceiveBatch(queueName, maxMessages, batchTimeoutInMilliseconds,
                           pollIntervalInMilliseconds, cancellationToken))
        {
            var startTime = Stopwatch.GetTimestamp();
            var batchSize = messageContext.Messages.Count();

            MessagesReceivedCounter.Add(batchSize,
                new KeyValuePair<string, object?>("queue.name", queueName));
            BatchSizeHistogram.Record(batchSize,
                new KeyValuePair<string, object?>("queue.name", queueName));

            using var messageActivity = ActivitySource.StartActivity("ProcessBatch", ActivityKind.Consumer);
            messageActivity?.SetTag("queue.name", queueName);
            messageActivity?.SetTag("batch.size", batchSize);
            messageActivity?.SetTag("message.ids", string.Join(",", messageContext.Messages.Select(x => x.Id)));

            yield return messageContext;

            var elapsed = Stopwatch.GetElapsedTime(startTime).TotalMilliseconds;
            MessageProcessingDuration.Record(elapsed,
                new KeyValuePair<string, object?>("queue.name", queueName),
                new KeyValuePair<string, object?>("batch.size", batchSize));
        }
    }

    public void ReceiveLater(Message message, TimeSpan timeSpan)
    {
        using var activity = ActivitySource.StartActivity("ReceiveLater", ActivityKind.Internal);
        activity?.SetTag("message.id", message.Id);
        activity?.SetTag("message.queue", message.Queue);
        activity?.SetTag("delay.timespan", timeSpan.ToString());
        activity?.SetTag("delay.seconds", timeSpan.TotalSeconds);

        try
        {
            _queue.ReceiveLater(message, timeSpan);
        }
        catch (Exception ex)
        {
            OperationErrorsCounter.Add(1,
                new KeyValuePair<string, object?>("operation", "ReceiveLater"),
                new KeyValuePair<string, object?>("queue.name", message.Queue));
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            activity?.AddException(ex);
            throw;
        }
    }

    public void ReceiveLater(Message message, DateTimeOffset time)
    {
        using var activity = ActivitySource.StartActivity("ReceiveLater", ActivityKind.Internal);
        activity?.SetTag("message.id", message.Id);
        activity?.SetTag("message.queue", message.Queue);
        activity?.SetTag("scheduled.time", time.ToString("O"));

        try
        {
            _queue.ReceiveLater(message, time);
        }
        catch (Exception ex)
        {
            OperationErrorsCounter.Add(1,
                new KeyValuePair<string, object?>("operation", "ReceiveLater"),
                new KeyValuePair<string, object?>("queue.name", message.Queue));
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            activity?.AddException(ex);
            throw;
        }
    }

    public void MoveToQueue(string queueName, Message message)
    {
        using var activity = ActivitySource.StartActivity("MoveToQueue", ActivityKind.Internal);
        activity?.SetTag("queue.name", queueName);
        activity?.SetTag("message.id", message.Id);
        activity?.SetTag("source.queue", message.Queue);

        try
        {
            _queue.MoveToQueue(queueName, message);
        }
        catch (Exception ex)
        {
            OperationErrorsCounter.Add(1,
                new KeyValuePair<string, object?>("operation", "MoveToQueue"),
                new KeyValuePair<string, object?>("queue.name", queueName));
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            activity?.AddException(ex);
            throw;
        }
    }

    public void Send(params Message[] messages)
    {
        using var activity = ActivitySource.StartActivity("SendBatch", ActivityKind.Producer);
        activity?.SetTag("message.count", messages.Length);
        activity?.SetTag("batch.size", messages.Length);

        try
        {
            MessagesSentCounter.Add(messages.Length);
            BatchSizeHistogram.Record(messages.Length);
            _queue.Send(messages);
        }
        catch (Exception ex)
        {
            OperationErrorsCounter.Add(1,
                new KeyValuePair<string, object?>("operation", "Send"),
                new KeyValuePair<string, object?>("batch.size", messages.Length));
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            activity?.AddException(ex);
            throw;
        }
    }

    public void Send(Message message)
    {
        using var activity = ActivitySource.StartActivity("Send", ActivityKind.Producer);
        activity?.SetTag("message.id", message.Id);
        activity?.SetTag("message.queue", message.Queue);
        activity?.SetTag("destination", message.Destination?.ToString());

        try
        {
            MessagesSentCounter.Add(1, new KeyValuePair<string, object?>("queue.name", message.Queue));
            _queue.Send(message);
        }
        catch (Exception ex)
        {
            OperationErrorsCounter.Add(1,
                new KeyValuePair<string, object?>("operation", "Send"),
                new KeyValuePair<string, object?>("queue.name", message.Queue));
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            activity?.AddException(ex);
            throw;
        }
    }

    public void Enqueue(Message message)
    {
        using var activity = ActivitySource.StartActivity("Enqueue", ActivityKind.Producer);
        activity?.SetTag("message.id", message.Id);
        activity?.SetTag("message.queue", message.Queue);

        try
        {
            MessagesEnqueuedCounter.Add(1, new KeyValuePair<string, object?>("queue.name", message.Queue));
            _queue.Enqueue(message);
        }
        catch (Exception ex)
        {
            OperationErrorsCounter.Add(1,
                new KeyValuePair<string, object?>("operation", "Enqueue"),
                new KeyValuePair<string, object?>("queue.name", message.Queue));
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            activity?.AddException(ex);
            throw;
        }
    }

    public IMessageStore Store => _queue.Store;
    public string[] Queues => _queue.Queues;
    public IPEndPoint Endpoint => _queue.Endpoint;
}