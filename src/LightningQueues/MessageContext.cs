using System.Collections.Generic;

namespace LightningQueues;

/// <summary>
/// Represents a context for processing a message within a queue.
/// </summary>
/// <remarks>
/// MessageContext encapsulates a message along with its processing context,
/// providing access to both the message content and operations that can be
/// performed on the message during processing.
/// </remarks>
public class MessageContext
{
    private readonly QueueContext _internalContext;

    /// <summary>
    /// Initializes a new instance of the <see cref="MessageContext"/> class.
    /// </summary>
    /// <param name="message">The message being processed.</param>
    /// <param name="queue">The queue containing the message.</param>
    internal MessageContext(Message message, Queue queue)
    {
        Message = message;
        _internalContext = new QueueContext(queue, message);
        QueueContext = _internalContext;
    }

    /// <summary>
    /// Gets the internal queue context for batch wiring.
    /// </summary>
    internal QueueContext InternalContext => _internalContext;

    /// <summary>
    /// Gets the message being processed.
    /// </summary>
    /// <remarks>
    /// This property provides access to the message content, headers, and metadata.
    /// </remarks>
    public Message Message { get; }
    
    /// <summary>
    /// Gets the queue context for the message.
    /// </summary>
    /// <remarks>
    /// The queue context provides operations for processing the message,
    /// such as marking it as received, moving it to another queue,
    /// or scheduling it for later processing.
    /// For messages received via <see cref="IQueue.ReceiveBatch"/>, this is a
    /// <see cref="BatchQueueContext"/> where <see cref="IQueueContext.CommitChanges"/>
    /// commits all messages in the batch atomically.
    /// </remarks>
    public IQueueContext QueueContext { get; internal set; }

    /// <summary>
    /// Commits all pending actions from multiple message contexts in a single atomic transaction.
    /// </summary>
    /// <param name="batch">The message contexts whose pending actions should be committed together.</param>
    /// <remarks>
    /// Use this method when processing a batch of messages from <see cref="IQueue.ReceiveBatch"/> to
    /// commit all acknowledgments in a single LMDB write transaction, rather than calling
    /// <see cref="IQueueContext.CommitChanges"/> on each message individually.
    /// </remarks>
    public static void CommitBatch(IEnumerable<MessageContext> batch)
    {
        var contexts = new List<QueueContext>();
        foreach (var msg in batch)
        {
            contexts.Add(msg.InternalContext);
        }
        LightningQueues.QueueContext.CommitBatch(contexts);
    }
}