using System;

namespace LightningQueues;

/// <summary>
/// Defines the operations available for processing a batch of messages in a queue.
/// Inherits all single-scope operations from <see cref="IQueueContext"/> (which apply
/// to every message in the batch) and adds subset operations that target specific
/// messages within the batch.
/// </summary>
public interface IBatchQueueContext : IQueueContext
{
    /// <summary>
    /// Gets the messages in this batch.
    /// </summary>
    Message[] Messages { get; }

    /// <summary>
    /// Schedules a subset of messages in this batch to be processed again after a specified delay.
    /// </summary>
    /// <param name="messages">The messages to defer.</param>
    /// <param name="timeSpan">The time to wait before making the messages available again.</param>
    void ReceiveLater(Message[] messages, TimeSpan timeSpan);

    /// <summary>
    /// Schedules a subset of messages in this batch to be processed again at a specific time.
    /// </summary>
    /// <param name="messages">The messages to defer.</param>
    /// <param name="time">The time at which the messages should be made available again.</param>
    void ReceiveLater(Message[] messages, DateTimeOffset time);

    /// <summary>
    /// Marks a subset of messages in this batch as successfully received and processed.
    /// </summary>
    /// <param name="messages">The messages to mark as received.</param>
    void SuccessfullyReceived(Message[] messages);

    /// <summary>
    /// Moves a subset of messages in this batch to a different queue.
    /// </summary>
    /// <param name="queueName">The name of the destination queue.</param>
    /// <param name="messages">The messages to move.</param>
    void MoveTo(string queueName, Message[] messages);
}
