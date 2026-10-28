using System;

namespace Bielu.PersistentQueues.Cluster;

/// <summary>
/// Thrown in <see cref="ReplicationMode.Synchronous"/> mode when a write could not be
/// replicated to the partition's replica, and by <see cref="IReplicationProtocol.SyncPartitionAsync"/>
/// when a partition could not be read from its source node.
/// </summary>
/// <remarks>
/// When a write fails, the message has already been stored (or forwarded) for the primary,
/// so it is not lost, but it is under-replicated. Retrying the write can create a duplicate.
/// </remarks>
public sealed class ClusterReplicationException : Exception
{
    /// <summary>
    /// Initializes a new instance of the <see cref="ClusterReplicationException"/> class.
    /// </summary>
    /// <param name="partitionKey">The partition that could not be replicated.</param>
    /// <param name="message">The error message.</param>
    public ClusterReplicationException(string partitionKey, string message) : this(partitionKey, message, null)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="ClusterReplicationException"/> class.
    /// </summary>
    /// <param name="partitionKey">The partition that could not be replicated.</param>
    /// <param name="message">The error message.</param>
    /// <param name="innerException">The exception that caused the failure, if any.</param>
    public ClusterReplicationException(string partitionKey, string message, Exception? innerException)
        : base(message, innerException)
    {
        PartitionKey = partitionKey;
    }

    /// <summary>
    /// Gets the partition (<c>queue:partition-N</c>) that could not be replicated.
    /// </summary>
    public string PartitionKey { get; }
}
