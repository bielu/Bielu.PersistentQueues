using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Runtime.CompilerServices;
using Bielu.PersistentQueues.OpenTelemetry.Instrumentation.Metrics;
using Bielu.PersistentQueues.OpenTelemetry.Instrumentation.Tracing;
using Bielu.PersistentQueues.Partitioning;

namespace Bielu.PersistentQueues.OpenTelemetry.Instrumentation;

/// <summary>
/// OpenTelemetry decorator for <see cref="IPartitionedQueue"/> that adds metrics and tracing
/// for all partition-specific operations.
/// </summary>
public class OtelPartitionedQueueDecorator : OtelQueueDecorator, IPartitionedQueue
{
    private readonly IPartitionedQueue _partitionedQueue;
    private readonly QueueMetrics _metrics;
    private readonly QueueActivitySource _activitySource;

    public OtelPartitionedQueueDecorator(
        IPartitionedQueue partitionedQueue,
        QueueMetrics queueMetrics,
        QueueActivitySource activitySource)
        : base(partitionedQueue, queueMetrics, activitySource)
    {
        _partitionedQueue = partitionedQueue;
        _metrics = queueMetrics;
        _activitySource = activitySource;

        // Total active (non-empty) partitions across all queues
        _activePartitionsGauge = _metrics.CreateActivePartitionsGauge(() =>
        {
            int total = 0;
            foreach (var (queueName, _) in GetPartitionedQueueNames())
                total += _partitionedQueue.GetActivePartitions(queueName).Length;
            return total;
        });

        // Per-queue: total partition count
        _partitionsPerQueueGauge = _metrics.CreatePartitionsPerQueueGauge(() =>
        {
            var measurements = new List<Measurement<int>>();
            foreach (var (queueName, partitionCount) in GetPartitionedQueueNames())
            {
                measurements.Add(new Measurement<int>(partitionCount,
                    new KeyValuePair<string, object?>("queue.name", queueName)));
            }
            return measurements;
        });

        // Per-queue: active (non-empty) partition count
        _activePartitionsPerQueueGauge = _metrics.CreateActivePartitionsPerQueueGauge(() =>
        {
            var measurements = new List<Measurement<int>>();
            foreach (var (queueName, _) in GetPartitionedQueueNames())
            {
                var active = _partitionedQueue.GetActivePartitions(queueName).Length;
                measurements.Add(new Measurement<int>(active,
                    new KeyValuePair<string, object?>("queue.name", queueName)));
            }
            return measurements;
        });

        // Per-partition depth: message count in each partition, tagged with queue.name and partition
        _partitionDepthGauge = _metrics.CreatePartitionDepthGauge(() =>
        {
            var measurements = new List<Measurement<long>>();
            foreach (var (queueName, partitionCount) in GetPartitionedQueueNames())
            {
                for (var i = 0; i < partitionCount; i++)
                {
                    var depth = _partitionedQueue.GetPartitionMessageCount(queueName, i);
                    measurements.Add(new Measurement<long>(depth,
                        new KeyValuePair<string, object?>("queue.name", queueName),
                        new KeyValuePair<string, object?>("partition", i)));
                }
            }
            return measurements;
        });
    }

    private readonly ObservableGauge<int> _activePartitionsGauge;
    private readonly ObservableGauge<int> _partitionsPerQueueGauge;
    private readonly ObservableGauge<int> _activePartitionsPerQueueGauge;
    private readonly ObservableGauge<long> _partitionDepthGauge;

    /// <summary>
    /// Returns the base names and partition counts of all partitioned queues currently known
    /// to the system. Uses the logical <see cref="IQueue.Queues"/> view (which already
    /// collapses partition sub-queues) and filters to those that have partitions.
    /// The partition count is computed once per queue per observation to avoid redundant calls.
    /// </summary>
    private IEnumerable<(string QueueName, int PartitionCount)> GetPartitionedQueueNames()
    {
        foreach (var queueName in _partitionedQueue.Queues)
        {
            var count = _partitionedQueue.GetPartitionCount(queueName);
            if (count > 0)
                yield return (queueName, count);
        }
    }

    /// <inheritdoc />
    public int GetPartitionCount(string queueName)
    {
        return _partitionedQueue.GetPartitionCount(queueName);
    }

    /// <inheritdoc />
    public IPartitionStrategy PartitionStrategy => _partitionedQueue.PartitionStrategy;

    /// <inheritdoc />
    public void CreatePartitionedQueue(string queueName, int partitionCount)
    {
        using var activity = _activitySource.StartActivity(ActivityNames.CreatePartitionedQueue, ActivityKind.Internal);
        QueueActivitySource.SetQueueTags(activity, queueName);
        QueueActivitySource.SetPartitionCountTag(activity, partitionCount);

        try
        {
            _partitionedQueue.CreatePartitionedQueue(queueName, partitionCount);
            _metrics.RecordPartitionCreated(queueName, partitionCount);
        }
        catch (Exception ex)
        {
            _metrics.RecordOperationError("CreatePartitionedQueue", queueName);
            _activitySource.RecordException(activity, ex);
            throw;
        }
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<IMessageContext> ReceiveFromPartition(string queueName, int partition,
        int pollIntervalInMilliseconds = 200,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        using var activity = _activitySource.StartActivity(ActivityNames.ReceiveFromPartition, ActivityKind.Consumer);
        QueueActivitySource.SetPartitionTags(activity, queueName, partition);
        QueueActivitySource.SetPollIntervalTag(activity, pollIntervalInMilliseconds);

        await foreach (var messageContext in _partitionedQueue.ReceiveFromPartition(
                           queueName, partition, pollIntervalInMilliseconds, cancellationToken))
        {
            _metrics.RecordPartitionReceived(1, queueName, partition);
            _metrics.RecordPartitionConsumerStarted(queueName, partition);

            var timeInQueue = Math.Max(0, (DateTime.UtcNow - messageContext.Message.SentAt).TotalMilliseconds);
            _metrics.RecordTimeInQueue(timeInQueue, queueName, partition);

            using var messageActivity =
                _activitySource.StartActivity(ActivityNames.ProcessMessage, ActivityKind.Consumer);
            QueueActivitySource.SetMessageTags(messageActivity, messageContext.Message.Id.MessageIdentifier, queueName);
            QueueActivitySource.SetPartitionTags(messageActivity, queueName, partition);

            yield return messageContext;

            _metrics.RecordPartitionConsumerStopped(queueName, partition);
        }
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<IBatchQueueContext> ReceiveBatchFromPartition(string queueName, int partition,
        int maxMessages = 0, int batchTimeoutInMilliseconds = 0, int pollIntervalInMilliseconds = 200,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        using var activity = _activitySource.StartActivity(ActivityNames.ReceiveBatchFromPartition, ActivityKind.Consumer);
        QueueActivitySource.SetPartitionTags(activity, queueName, partition);
        QueueActivitySource.SetPollIntervalTag(activity, pollIntervalInMilliseconds);
        activity?.SetTag("max.messages", maxMessages);
        activity?.SetTag("batch.timeout", batchTimeoutInMilliseconds);

        await foreach (var batchContext in _partitionedQueue.ReceiveBatchFromPartition(
                           queueName, partition, maxMessages, batchTimeoutInMilliseconds,
                           pollIntervalInMilliseconds, cancellationToken))
        {
            var batchSize = batchContext.Messages.Length;
            _metrics.RecordPartitionReceived(batchSize, queueName, partition);
            _metrics.RecordBatchSize(batchSize, queueName);

            var now = DateTime.UtcNow;
            foreach (var msg in batchContext.Messages)
            {
                var timeInQueue = Math.Max(0, (now - msg.SentAt).TotalMilliseconds);
                _metrics.RecordTimeInQueue(timeInQueue, queueName, partition);
            }

            using var batchActivity =
                _activitySource.StartActivity(ActivityNames.ProcessBatch, ActivityKind.Consumer);
            QueueActivitySource.SetBatchTags(batchActivity, batchSize, queueName);
            QueueActivitySource.SetPartitionTags(batchActivity, queueName, partition);

            yield return batchContext;
        }
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<IMessageContext> ReceiveFromPartitions(string queueName, int[] partitions,
        int pollIntervalInMilliseconds = 200,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        using var activity = _activitySource.StartActivity(ActivityNames.ReceiveFromPartitions, ActivityKind.Consumer);
        QueueActivitySource.SetQueueTags(activity, queueName);
        activity?.SetTag("partitions", string.Join(",", partitions));
        QueueActivitySource.SetPollIntervalTag(activity, pollIntervalInMilliseconds);

        await foreach (var messageContext in _partitionedQueue.ReceiveFromPartitions(
                           queueName, partitions, pollIntervalInMilliseconds, cancellationToken))
        {
            _metrics.RecordMessagesReceived(1, queueName);

            var timeInQueue = Math.Max(0, (DateTime.UtcNow - messageContext.Message.SentAt).TotalMilliseconds);
            _metrics.RecordTimeInQueue(timeInQueue, queueName);

            yield return messageContext;
        }
    }

    /// <inheritdoc />
    public void EnqueueToPartition(Message message, int partition)
    {
        var queueName = message.QueueString ?? string.Empty;
        using var activity = _activitySource.StartActivity(ActivityNames.EnqueueToPartition, ActivityKind.Producer);
        QueueActivitySource.SetPartitionTags(activity, queueName, partition, message.PartitionKeyString);
        QueueActivitySource.SetMessageTags(activity, message.Id.MessageIdentifier, queueName);

        try
        {
            var startTime = Stopwatch.GetTimestamp();

            _metrics.RecordPartitionProducerStarted(queueName);
            _partitionedQueue.EnqueueToPartition(message, partition);
            _metrics.RecordPartitionProducerStopped(queueName);

            var elapsed = Stopwatch.GetElapsedTime(startTime).TotalMilliseconds;
            _metrics.RecordPartitionEnqueued(queueName, partition, message.PartitionKeyString);
            _metrics.RecordPartitionEnqueueDuration(elapsed, queueName, partition);
        }
        catch (Exception ex)
        {
            _metrics.RecordPartitionProducerStopped(queueName);
            _metrics.RecordOperationError("EnqueueToPartition", queueName);
            _activitySource.RecordException(activity, ex);
            throw;
        }
    }

    /// <inheritdoc />
    public int ResolvePartition(Message message)
    {
        var queueName = message.QueueString ?? string.Empty;
        using var activity = _activitySource.StartActivity(ActivityNames.ResolvePartition, ActivityKind.Internal);
        QueueActivitySource.SetQueueTags(activity, queueName);

        try
        {
            var partition = _partitionedQueue.ResolvePartition(message);
            QueueActivitySource.SetPartitionTags(activity, queueName, partition, message.PartitionKeyString);
            return partition;
        }
        catch (Exception ex)
        {
            _metrics.RecordOperationError("ResolvePartition", queueName);
            _activitySource.RecordException(activity, ex);
            throw;
        }
    }

    /// <inheritdoc />
    public long GetPartitionMessageCount(string queueName, int partition)
    {
        return _partitionedQueue.GetPartitionMessageCount(queueName, partition);
    }

    /// <inheritdoc />
    public int[] GetActivePartitions(string queueName)
    {
        return _partitionedQueue.GetActivePartitions(queueName);
    }

    /// <inheritdoc />
    public int[] GetAvailablePartitions(string queueName)
    {
        return _partitionedQueue.GetAvailablePartitions(queueName);
    }

    /// <inheritdoc />
    public void EnablePartitioning(string queueName, int partitionCount)
    {
        using var activity = _activitySource.StartActivity(ActivityNames.EnablePartitioning, ActivityKind.Internal);
        QueueActivitySource.SetQueueTags(activity, queueName);
        QueueActivitySource.SetPartitionCountTag(activity, partitionCount);

        try
        {
            _partitionedQueue.EnablePartitioning(queueName, partitionCount);
            _metrics.RecordPartitionCreated(queueName, partitionCount);
        }
        catch (Exception ex)
        {
            _metrics.RecordOperationError("EnablePartitioning", queueName);
            _activitySource.RecordException(activity, ex);
            throw;
        }
    }

    /// <inheritdoc />
    public void DisablePartitioning(string queueName)
    {
        using var activity = _activitySource.StartActivity(ActivityNames.DisablePartitioning, ActivityKind.Internal);
        QueueActivitySource.SetQueueTags(activity, queueName);

        try
        {
            _partitionedQueue.DisablePartitioning(queueName);
        }
        catch (Exception ex)
        {
            _metrics.RecordOperationError("DisablePartitioning", queueName);
            _activitySource.RecordException(activity, ex);
            throw;
        }
    }

    /// <inheritdoc />
    public void Repartition(string queueName, int newPartitionCount)
    {
        using var activity = _activitySource.StartActivity(ActivityNames.Repartition, ActivityKind.Internal);
        QueueActivitySource.SetQueueTags(activity, queueName);
        QueueActivitySource.SetPartitionCountTag(activity, newPartitionCount);

        try
        {
            var oldPartitionCount = _partitionedQueue.GetPartitionCount(queueName);
            _partitionedQueue.Repartition(queueName, newPartitionCount);

            // Only record newly created partitions (expanding); no metric for shrinking or no-op
            var delta = newPartitionCount - oldPartitionCount;
            if (delta > 0)
            {
                _metrics.RecordPartitionCreated(queueName, delta);
            }
        }
        catch (Exception ex)
        {
            _metrics.RecordOperationError("Repartition", queueName);
            _activitySource.RecordException(activity, ex);
            throw;
        }
    }
}
