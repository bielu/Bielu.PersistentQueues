namespace Bielu.PersistentQueues;

/// <summary>
/// Contains constants and helpers for dead letter queue naming.
/// </summary>
/// <remarks>
/// A dead letter queue (DLQ) is a special queue that receives messages which
/// could not be processed or delivered successfully after all retry attempts
/// have been exhausted, or which were explicitly moved there by a consumer.
/// Each source queue has a corresponding DLQ named with the
/// <see cref="DeadLetterSuffix"/> suffix (e.g., <c>orders:dead-letter</c>).
/// </remarks>
public static class DeadLetterConstants
{
    /// <summary>
    /// The suffix appended to a source queue name to form its dead letter queue name.
    /// </summary>
    public const string DeadLetterSuffix = ":dead-letter";

    /// <summary>
    /// Returns the dead letter queue name for the given source queue name.
    /// </summary>
    /// <param name="queueName">The source queue name.</param>
    /// <returns>The corresponding dead letter queue name.</returns>
    public static string GetDeadLetterQueueName(string queueName)
        => queueName + DeadLetterSuffix;

    /// <summary>
    /// Determines whether the given queue name is a dead letter queue.
    /// </summary>
    /// <param name="queueName">The queue name to check.</param>
    /// <returns><c>true</c> if the queue is a dead letter queue; otherwise, <c>false</c>.</returns>
    public static bool IsDeadLetterQueue(string queueName)
        => queueName.EndsWith(DeadLetterSuffix, System.StringComparison.Ordinal);
}
