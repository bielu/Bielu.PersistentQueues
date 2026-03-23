using System;
using System.Collections.Generic;

namespace LightningQueues;

/// <summary>
/// A queue context decorator that commits all messages in a batch atomically.
/// </summary>
/// <remarks>
/// When messages are received via <see cref="IQueue.ReceiveBatch"/>, each message's
/// <see cref="MessageContext.QueueContext"/> is a <see cref="BatchQueueContext"/>.
/// Per-message operations like <see cref="SuccessfullyReceived"/> and <see cref="MoveTo"/>
/// apply to the individual message, while <see cref="CommitChanges"/> commits all
/// pending actions from every message in the batch in a single atomic transaction.
/// </remarks>
internal class BatchQueueContext : IQueueContext
{
    private readonly QueueContext _perMessageContext;
    private readonly IReadOnlyList<QueueContext> _batchContexts;

    internal BatchQueueContext(QueueContext perMessageContext, IReadOnlyList<QueueContext> batchContexts)
    {
        _perMessageContext = perMessageContext;
        _batchContexts = batchContexts;
    }

    /// <summary>
    /// Commits all pending actions from every message in the batch in a single atomic transaction.
    /// </summary>
    public void CommitChanges()
    {
        QueueContext.CommitBatch(_batchContexts);
    }

    /// <inheritdoc />
    public void Send(Message message) => _perMessageContext.Send(message);

    /// <inheritdoc />
    public void ReceiveLater(TimeSpan timeSpan) => _perMessageContext.ReceiveLater(timeSpan);

    /// <inheritdoc />
    public void ReceiveLater(DateTimeOffset time) => _perMessageContext.ReceiveLater(time);

    /// <inheritdoc />
    public void SuccessfullyReceived() => _perMessageContext.SuccessfullyReceived();

    /// <inheritdoc />
    public void MoveTo(string queueName) => _perMessageContext.MoveTo(queueName);

    /// <inheritdoc />
    public void Enqueue(Message message) => _perMessageContext.Enqueue(message);
}
