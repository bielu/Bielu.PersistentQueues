using System.Collections.Generic;
using System.Diagnostics.Metrics;

namespace Bielu.PersistentQueues;

/// <summary>
/// Provides BCL-based metrics instrumentation for dead letter queue operations.
/// These metrics are emitted via the <c>BieluPersistentQueues</c> meter and are
/// automatically collected by any registered OpenTelemetry meter provider.
/// </summary>
internal static class DeadLetterDiagnostics
{
    internal const string MeterName = "BieluPersistentQueues";
    internal const string MessagesDeadLetteredMetricName = "bielupersistentqueues.messages.dead_lettered";

    private static readonly Meter Meter = new(MeterName, "1.0.0");

    private static readonly Counter<long> MessagesDeadLetteredCounter = Meter.CreateCounter<long>(
        MessagesDeadLetteredMetricName,
        description: "Total number of messages moved to a dead letter queue");

    /// <summary>
    /// Records that a message has been moved to a dead letter queue.
    /// </summary>
    /// <param name="sourceQueue">The name of the source queue the message came from.</param>
    /// <param name="reason">The reason the message was dead-lettered (see <see cref="Reasons"/>).</param>
    internal static void RecordMessageDeadLettered(string sourceQueue, string reason)
    {
        MessagesDeadLetteredCounter.Add(1,
            new KeyValuePair<string, object?>("queue.name", sourceQueue),
            new KeyValuePair<string, object?>("reason", reason));
    }

    /// <summary>
    /// Well-known reason strings for dead-lettering a message.
    /// </summary>
    internal static class Reasons
    {
        /// <summary>The consumer explicitly called <c>MoveToDeadLetter()</c>.</summary>
        public const string Manual = "manual";

        /// <summary>The message exceeded its <c>MaxAttempts</c> processing limit.</summary>
        public const string MaxProcessingAttempts = "max_processing_attempts";

        /// <summary>The message could not be delivered after exhausting all send retries.</summary>
        public const string SendFailed = "send_failed";
    }
}
