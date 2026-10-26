using System;

namespace Bielu.PersistentQueues.Cluster;

/// <summary>
/// Thrown in <see cref="ReplicationMode.Synchronous"/> mode when a write could not be
/// replicated to the partition's replica.
/// </summary>
/// <remarks>
/// The message has already been stored (or forwarded) for the primary when this is thrown,
/// so it is not lost, but it is under-replicated. Retrying the write can create a duplicate.
/// </remarks>
public sealed class ClusterReplicationException : Exception
{
    /// <summary>
    /// Initializes a new instance of the <see cref="ClusterReplicationException"/> class.
    /// </summary>
    /// <param name="partitionKey">The partition that could not be replicated.</param>
    /// <param name="message">The error message.</param>
    public ClusterReplicationException(string partitionKey, string message) : base(message)
    {
        PartitionKey = partitionKey;
    }

    /// <summary>
    /// Gets the partition (<c>queue:partition-N</c>) that could not be replicated.
    /// </summary>
    public string PartitionKey { get; }
}
