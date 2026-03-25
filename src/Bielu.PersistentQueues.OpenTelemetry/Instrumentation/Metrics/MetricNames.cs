namespace Bielu.PersistentQueues.OpenTelemetry.Instrumentation.Metrics;

internal static class MetricNames
{
    public const string MeterName = "BieluPersistentQueues";

    public const string MessagesSent = "bielupersistentqueues.messages.sent";
    public const string MessagesReceived = "bielupersistentqueues.messages.received";
    public const string MessagesEnqueued = "bielupersistentqueues.messages.enqueued";
    public const string OperationErrors = "bielupersistentqueues.operations.errors";
    public const string MessageProcessingDuration = "bielupersistentqueues.message.processing.duration";
    public const string EnqueueDuration = "bielupersistentqueues.enqueue.duration";
    public const string DequeueDuration = "bielupersistentqueues.dequeue.duration";
    public const string BatchSize = "bielupersistentqueues.batch.size";
    public const string QueuesActive = "bielupersistentqueues.queues.active";
}
