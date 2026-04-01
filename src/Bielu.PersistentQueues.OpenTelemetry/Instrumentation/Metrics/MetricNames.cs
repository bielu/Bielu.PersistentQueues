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
    public const string FailedBatchCount = "bielupersistentqueues.batch.count";

    // Partitioning metrics
    public const string PartitionEnqueued = "bielupersistentqueues.partition.messages.enqueued";
    public const string PartitionReceived = "bielupersistentqueues.partition.messages.received";
    public const string PartitionCreated = "bielupersistentqueues.partition.created";
    public const string PartitionEnqueueDuration = "bielupersistentqueues.partition.enqueue.duration";
}
