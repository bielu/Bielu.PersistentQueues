namespace Bielu.PersistentQueues;

/// <summary>
/// Configuration options for dead letter queue behaviour.
/// </summary>
/// <remarks>
/// These options control whether messages that cannot be processed or delivered
/// are automatically moved to a dead letter queue. When disabled, failed messages
/// are simply removed from the outgoing store (send failures) or the
/// <see cref="IQueueContext.MoveToDeadLetter"/> call throws
/// <see cref="System.InvalidOperationException"/>.
/// </remarks>
public sealed class DeadLetterOptions
{
    /// <summary>
    /// Gets or sets a value indicating whether the dead letter queue is enabled.
    /// Defaults to <c>false</c>.
    /// </summary>
    /// <remarks>
    /// When <c>true</c>:
    /// <list type="bullet">
    ///   <item>Messages that exceed <see cref="Message.MaxAttempts"/> during
    ///         <see cref="IQueueContext.ReceiveLater(System.TimeSpan)"/> are automatically
    ///         moved to the dead letter queue.</item>
    ///   <item>Outgoing messages that fail all send retries are stored in the dead letter queue.</item>
    ///   <item>Consumers may explicitly call <see cref="IQueueContext.MoveToDeadLetter"/>.</item>
    /// </list>
    /// When <c>false</c>, all of the above operations are no-ops or throw.
    /// </remarks>
    public bool Enabled { get; set; }
}
