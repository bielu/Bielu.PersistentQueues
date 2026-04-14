namespace Bielu.PersistentQueues.OpenTelemetry.Instrumentation.Metrics;

internal static class MetricNames
{
    public const string MeterName = "BieluPersistentQueues";

    public const string MessagesSent = "bielupersistentqueues.messages.sent";
    public const string MessagesFailed = "bielupersistentqueues.messages.failed";
    public const string MessagesReceived = "bielupersistentqueues.messages.received";
    public const string MessagesEnqueued = "bielupersistentqueues.messages.enqueued";
    public const string OperationErrors = "bielupersistentqueues.operations.errors";
    public const string MessageProcessingDuration = "bielupersistentqueues.message.processing.duration";
    public const string EnqueueDuration = "bielupersistentqueues.enqueue.duration";
    public const string DequeueDuration = "bielupersistentqueues.dequeue.duration";
    public const string BatchSize = "bielupersistentqueues.batch.size";
    public const string QueuesActive = "bielupersistentqueues.queues.active";
    public const string BatchCount = "bielupersistentqueues.batch.count";
    public const string FailedBatchCount = "bielupersistentqueues.batch.failed.count";
    public const string StorageUsedBytes = "bielupersistentqueues.storage.used_bytes";
    public const string StorageTotalBytes = "bielupersistentqueues.storage.total_bytes";
    public const string StorageUsagePercent = "bielupersistentqueues.storage.usage_percent";

    // Partitioning metrics
    public const string PartitionEnqueued = "bielupersistentqueues.partition.messages.enqueued";
    public const string PartitionReceived = "bielupersistentqueues.partition.messages.received";
    public const string PartitionCreated = "bielupersistentqueues.partition.created";
    public const string PartitionEnqueueDuration = "bielupersistentqueues.partition.enqueue.duration";
    public const string PartitionsActive = "bielupersistentqueues.partitions.active";
    public const string PartitionsPerQueue = "bielupersistentqueues.partitions.per_queue";
    public const string PartitionsActivePerQueue = "bielupersistentqueues.partitions.active_per_queue";
    public const string PartitionConsumersActive = "bielupersistentqueues.partition.consumers.active";
    public const string PartitionProducersActive = "bielupersistentqueues.partition.producers.active";

    // Queue depth metrics
    public const string QueueDepth = "bielupersistentqueues.queue.depth";
    public const string PartitionDepth = "bielupersistentqueues.partition.depth";
    public const string TimeInQueue = "bielupersistentqueues.message.time_in_queue";

    // Dead letter queue metrics
    public const string MessagesDeadLettered = "bielupersistentqueues.messages.dead_lettered";
    public const string DeadLetterQueueDepth = "bielupersistentqueues.dead_letter.queue.depth";
}
