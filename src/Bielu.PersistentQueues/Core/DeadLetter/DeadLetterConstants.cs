namespace Bielu.PersistentQueues;

/// <summary>
/// Contains constants and helpers for the dead letter queue.
/// </summary>
/// <remarks>
/// There is a single, shared dead letter queue named <see cref="QueueName"/>.
/// Every message that is dead-lettered — regardless of its source queue — is
/// placed into this queue. The <c>original-queue</c> header on each message
/// records which queue the message originally belonged to.
/// </remarks>
public static class DeadLetterConstants
{
    /// <summary>
    /// The name of the shared dead letter queue.
    /// </summary>
    public const string QueueName = "dead-letter";

    /// <summary>
    /// Determines whether the given queue name is the dead letter queue.
    /// </summary>
    /// <param name="queueName">The queue name to check.</param>
    /// <returns><c>true</c> if the queue is the dead letter queue; otherwise, <c>false</c>.</returns>
    public static bool IsDeadLetterQueue(string queueName)
        => string.Equals(queueName, QueueName, System.StringComparison.Ordinal);
}
