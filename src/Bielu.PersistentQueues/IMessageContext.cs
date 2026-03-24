namespace Bielu.PersistentQueues;

/// <summary>
/// Defines the context for processing a single message within a queue.
/// </summary>
/// <remarks>
/// IMessageContext encapsulates a message along with its processing context,
/// providing access to both the message content and operations that can be
/// performed on the message during processing.
/// </remarks>
public interface IMessageContext
{
    /// <summary>
    /// Gets the message being processed.
    /// </summary>
    /// <remarks>
    /// This property provides access to the message content, headers, and metadata.
    /// </remarks>
    Message Message { get; }

    /// <summary>
    /// Gets the queue context for the message.
    /// </summary>
    /// <remarks>
    /// The queue context provides operations for processing the message,
    /// such as marking it as received, moving it to another queue,
    /// or scheduling it for later processing.
    /// </remarks>
    IQueueContext QueueContext { get; }
}
