using System.Diagnostics.Metrics;

namespace Bielu.PersistentQueues.OpenTelemetry.Instrumentation.Metrics;

public sealed class QueueMetrics
{
    private readonly Meter _meter;
    private readonly Counter<long> _messagesSentCounter;
    private readonly Counter<long> _messagesReceivedCounter;
    private readonly Counter<long> _messagesEnqueuedCounter;
    private readonly Counter<long> _messagesFailedCounter;
    private readonly Counter<long> _operationErrorsCounter;
    private readonly Histogram<double> _messageProcessingDuration;
    private readonly Histogram<double> _enqueueDuration;
    private readonly Histogram<double> _dequeueDuration;
    private readonly Histogram<int> _batchSizeHistogram;
    private readonly Counter<long> _messagesBatchCounter;
    private readonly Counter<long> _messagesBatchFailedCounter;
    private readonly Counter<long> _partitionEnqueuedCounter;
    private readonly Counter<long> _partitionReceivedCounter;
    private readonly Counter<long> _partitionCreatedCounter;
    private readonly Histogram<double> _partitionEnqueueDuration;
    private readonly UpDownCounter<long> _partitionConsumersActive;
    private readonly UpDownCounter<long> _partitionProducersActive;
    private readonly Histogram<double> _timeInQueueHistogram;

    public QueueMetrics()
    {
        _meter = new Meter(MetricNames.MeterName, "1.0.0");

        _messagesSentCounter = _meter.CreateCounter<long>(
            MetricNames.MessagesSent,
            description: "Total number of messages sent");

        _messagesReceivedCounter = _meter.CreateCounter<long>(
            MetricNames.MessagesReceived,
            description: "Total number of messages received");
        _messagesFailedCounter = _meter.CreateCounter<long>(
            MetricNames.MessagesFailed,
            description: "Total number of failed messages");
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

        _enqueueDuration = _meter.CreateHistogram<double>(
            MetricNames.EnqueueDuration,
            unit: "ms",
            description: "Duration of enqueue operation (writing message to storage)");

        _dequeueDuration = _meter.CreateHistogram<double>(
            MetricNames.DequeueDuration,
            unit: "ms",
            description: "Duration of dequeue operation (reading message from storage)");

        _batchSizeHistogram = _meter.CreateHistogram<int>(
            MetricNames.BatchSize,
            description: "Number of messages in a batch");
        _messagesBatchCounter = _meter.CreateCounter<long>(
            MetricNames.BatchCount,
            description: "Total number of batches processed");
        _messagesBatchFailedCounter = _meter.CreateCounter<long>(
            MetricNames.FailedBatchCount,
            description: "Total number of failed batches");

        // Partitioning metrics
        _partitionEnqueuedCounter = _meter.CreateCounter<long>(
            MetricNames.PartitionEnqueued,
            description: "Total number of messages enqueued to partitions");

        _partitionReceivedCounter = _meter.CreateCounter<long>(
            MetricNames.PartitionReceived,
            description: "Total number of messages received from partitions");

        _partitionCreatedCounter = _meter.CreateCounter<long>(
            MetricNames.PartitionCreated,
            description: "Total number of partitions created");

        _partitionEnqueueDuration = _meter.CreateHistogram<double>(
            MetricNames.PartitionEnqueueDuration,
            unit: "ms",
            description: "Duration of enqueue operation to a partition");

        _partitionConsumersActive = _meter.CreateUpDownCounter<long>(
            MetricNames.PartitionConsumersActive,
            description: "Number of currently active partition consumers");

        _partitionProducersActive = _meter.CreateUpDownCounter<long>(
            MetricNames.PartitionProducersActive,
            description: "Number of currently active partition producers");

        _timeInQueueHistogram = _meter.CreateHistogram<double>(
            MetricNames.TimeInQueue,
            unit: "ms",
            description: "Time a message spent waiting in the queue before being received");
    }

    public ObservableGauge<int> CreateActiveQueuesGauge(Func<int> observeValue)
    {
        return _meter.CreateObservableGauge(
            MetricNames.QueuesActive,
            () => new Measurement<int>(observeValue()),
            description: "Number of active queues");
    }

    public ObservableGauge<long> CreateStorageUsedBytesGauge(Func<long> observeValue)
    {
        return _meter.CreateObservableGauge(
            MetricNames.StorageUsedBytes,
            () => new Measurement<long>(observeValue()),
            unit: "By",
            description: "Number of bytes currently used by the storage");
    }

    public ObservableGauge<long> CreateStorageTotalBytesGauge(Func<long> observeValue)
    {
        return _meter.CreateObservableGauge(
            MetricNames.StorageTotalBytes,
            () => new Measurement<long>(observeValue()),
            unit: "By",
            description: "Total number of bytes allocated for the storage");
    }

    public ObservableGauge<double> CreateStorageUsagePercentGauge(Func<double> observeValue)
    {
        return _meter.CreateObservableGauge(
            MetricNames.StorageUsagePercent,
            () => new Measurement<double>(observeValue()),
            unit: "%",
            description: "Percentage of storage currently in use");
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
    public void RecordBatchProcessed(string? queueName = null)
    {
        if (queueName != null)
        {
            _messagesBatchCounter.Add(1, new KeyValuePair<string, object?>("queue.name", queueName));
        }
        else
        {
            _messagesBatchCounter.Add(1);
        }
    }
    public void RecordBatchFailed(string? queueName = null, int itemCount = 0)
    {
        if (queueName != null)
        {
            _messagesBatchFailedCounter.Add(1, new KeyValuePair<string, object?>("queue.name", queueName));
            if (itemCount != 0)
            {
                _messagesFailedCounter.Add(itemCount, new KeyValuePair<string, object?>("queue.name", queueName));
            }
        }
        else
        {
            _messagesBatchFailedCounter.Add(1);
            if (itemCount != 0)
            {
                _messagesFailedCounter.Add(itemCount);
            }
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

    public void RecordEnqueueDuration(double durationMs, string queueName, int messageCount = 1)
    {
        if (messageCount > 1)
        {
            _enqueueDuration.Record(durationMs,
                new KeyValuePair<string, object?>("queue.name", queueName),
                new KeyValuePair<string, object?>("message.count", messageCount));
        }
        else
        {
            _enqueueDuration.Record(durationMs,
                new KeyValuePair<string, object?>("queue.name", queueName));
        }
    }

    public void RecordDequeueDuration(double durationMs, string queueName, int messageCount = 1)
    {
        if (messageCount > 1)
        {
            _dequeueDuration.Record(durationMs,
                new KeyValuePair<string, object?>("queue.name", queueName),
                new KeyValuePair<string, object?>("message.count", messageCount));
        }
        else
        {
            _dequeueDuration.Record(durationMs,
                new KeyValuePair<string, object?>("queue.name", queueName));
        }
    }

    public void RecordPartitionEnqueued(string queueName, int partition, string? partitionKey = null)
    {
        var tags = new List<KeyValuePair<string, object?>>
        {
            new("queue.name", queueName),
            new("partition", partition)
        };

        if (partitionKey != null)
        {
            tags.Add(new KeyValuePair<string, object?>("partition.key", partitionKey));
        }

        _partitionEnqueuedCounter.Add(1, tags.ToArray());
    }

    public void RecordPartitionReceived(int count, string queueName, int partition)
    {
        _partitionReceivedCounter.Add(count,
            new KeyValuePair<string, object?>("queue.name", queueName),
            new KeyValuePair<string, object?>("partition", partition));
    }

    public void RecordPartitionCreated(string queueName, int partitionCount)
    {
        _partitionCreatedCounter.Add(partitionCount,
            new KeyValuePair<string, object?>("queue.name", queueName));
    }

    public void RecordPartitionEnqueueDuration(double durationMs, string queueName, int partition)
    {
        _partitionEnqueueDuration.Record(durationMs,
            new KeyValuePair<string, object?>("queue.name", queueName),
            new KeyValuePair<string, object?>("partition", partition));
    }

    public ObservableGauge<int> CreateActivePartitionsGauge(Func<int> observeValue)
    {
        return _meter.CreateObservableGauge(
            MetricNames.PartitionsActive,
            () => new Measurement<int>(observeValue()),
            description: "Total number of active (non-empty) partitions across all queues");
    }

    public ObservableGauge<int> CreatePartitionsPerQueueGauge(Func<IEnumerable<Measurement<int>>> observeValues)
    {
        return _meter.CreateObservableGauge(
            MetricNames.PartitionsPerQueue,
            observeValues,
            description: "Number of partitions per queue");
    }

    public ObservableGauge<int> CreateActivePartitionsPerQueueGauge(Func<IEnumerable<Measurement<int>>> observeValues)
    {
        return _meter.CreateObservableGauge(
            MetricNames.PartitionsActivePerQueue,
            observeValues,
            description: "Number of active (non-empty) partitions per queue");
    }

    public ObservableGauge<long> CreateQueueDepthGauge(Func<IEnumerable<Measurement<long>>> observeValues)
    {
        return _meter.CreateObservableGauge(
            MetricNames.QueueDepth,
            observeValues,
            description: "Total number of messages currently in each queue");
    }

    public void RecordTimeInQueue(double durationMs, string queueName)
    {
        _timeInQueueHistogram.Record(durationMs,
            new KeyValuePair<string, object?>("queue.name", queueName));
    }

    /// <summary>
    /// Creates an observable gauge that reports the current number of messages in each
    /// dead letter queue as separate measurements tagged with the DLQ name.
    /// </summary>
    public ObservableGauge<long> CreateDeadLetterQueueDepthGauge(Func<IEnumerable<Measurement<long>>> observeValues)
    {
        return _meter.CreateObservableGauge(
            MetricNames.DeadLetterQueueDepth,
            observeValues,
            description: "Number of messages currently waiting in dead letter queues");
    }

    public void RecordPartitionConsumerStarted(string queueName, int partition)
    {
        _partitionConsumersActive.Add(1,
            new KeyValuePair<string, object?>("queue.name", queueName),
            new KeyValuePair<string, object?>("partition", partition));
    }

    public void RecordPartitionConsumerStopped(string queueName, int partition)
    {
        _partitionConsumersActive.Add(-1,
            new KeyValuePair<string, object?>("queue.name", queueName),
            new KeyValuePair<string, object?>("partition", partition));
    }

    public void RecordPartitionProducerStarted(string queueName)
    {
        _partitionProducersActive.Add(1,
            new KeyValuePair<string, object?>("queue.name", queueName));
    }

    public void RecordPartitionProducerStopped(string queueName)
    {
        _partitionProducersActive.Add(-1,
            new KeyValuePair<string, object?>("queue.name", queueName));
    }
}
