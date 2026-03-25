using System.Diagnostics.Metrics;

namespace Bielu.PersistentQueues.OpenTelemetry.Instrumentation.Metrics;

internal sealed class QueueMetrics
{
    private readonly Meter _meter;
    private readonly Counter<long> _messagesSentCounter;
    private readonly Counter<long> _messagesReceivedCounter;
    private readonly Counter<long> _messagesEnqueuedCounter;
    private readonly Counter<long> _operationErrorsCounter;
    private readonly Histogram<double> _messageProcessingDuration;
    private readonly Histogram<int> _batchSizeHistogram;

    public QueueMetrics()
    {
        _meter = new Meter(MetricNames.MeterName, "1.0.0");

        _messagesSentCounter = _meter.CreateCounter<long>(
            MetricNames.MessagesSent,
            description: "Total number of messages sent");

        _messagesReceivedCounter = _meter.CreateCounter<long>(
            MetricNames.MessagesReceived,
            description: "Total number of messages received");

        _messagesEnqueuedCounter = _meter.CreateCounter<long>(
            MetricNames.MessagesEnqueued,
            description: "Total number of messages enqueued");

        _operationErrorsCounter = _meter.CreateCounter<long>(
            MetricNames.OperationErrors,
            description: "Total number of operation errors");

        _messageProcessingDuration = _meter.CreateHistogram<double>(
            MetricNames.MessageProcessingDuration,
            unit: "ms",
            description: "Duration of message processing");

        _batchSizeHistogram = _meter.CreateHistogram<int>(
            MetricNames.BatchSize,
            description: "Number of messages in a batch");
    }

    public ObservableGauge<int> CreateActiveQueuesGauge(Func<int> observeValue)
    {
        return _meter.CreateObservableGauge(
            MetricNames.QueuesActive,
            () => new Measurement<int>(observeValue()),
            description: "Number of active queues");
    }

    public void RecordMessageSent(string? queueName = null)
    {
        if (queueName != null)
        {
            _messagesSentCounter.Add(1, new KeyValuePair<string, object?>("queue.name", queueName));
        }
        else
        {
            _messagesSentCounter.Add(1);
        }
    }

    public void RecordMessagesSent(int count)
    {
        _messagesSentCounter.Add(count);
    }

    public void RecordMessagesReceived(int count, string queueName)
    {
        _messagesReceivedCounter.Add(count, new KeyValuePair<string, object?>("queue.name", queueName));
    }

    public void RecordMessageEnqueued(string queueName)
    {
        _messagesEnqueuedCounter.Add(1, new KeyValuePair<string, object?>("queue.name", queueName));
    }

    public void RecordOperationError(string operation, string? queueName = null, int? batchSize = null)
    {
        var tags = new List<KeyValuePair<string, object?>>
        {
            new("operation", operation)
        };

        if (queueName != null)
        {
            tags.Add(new KeyValuePair<string, object?>("queue.name", queueName));
        }

        if (batchSize.HasValue)
        {
            tags.Add(new KeyValuePair<string, object?>("batch.size", batchSize.Value));
        }

        _operationErrorsCounter.Add(1, tags.ToArray());
    }

    public void RecordProcessingDuration(double durationMs, string queueName, int? batchSize = null)
    {
        if (batchSize.HasValue)
        {
            _messageProcessingDuration.Record(durationMs,
                new KeyValuePair<string, object?>("queue.name", queueName),
                new KeyValuePair<string, object?>("batch.size", batchSize.Value));
        }
        else
        {
            _messageProcessingDuration.Record(durationMs,
                new KeyValuePair<string, object?>("queue.name", queueName));
        }
    }

    public void RecordBatchSize(int size, string? queueName = null)
    {
        if (queueName != null)
        {
            _batchSizeHistogram.Record(size, new KeyValuePair<string, object?>("queue.name", queueName));
        }
        else
        {
            _batchSizeHistogram.Record(size);
        }
    }
}
