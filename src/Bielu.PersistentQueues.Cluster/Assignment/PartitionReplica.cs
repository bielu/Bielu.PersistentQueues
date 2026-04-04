using System;

namespace Bielu.PersistentQueues.Cluster;

/// <summary>
/// Represents the assignment of a single partition to a primary and replica node.
/// </summary>
/// <remarks>
/// Each partition has exactly one primary node that handles reads and writes,
/// and one replica node that maintains a copy for fault tolerance.
/// The <see cref="Epoch"/> is incremented on every assignment change to detect stale primaries.
/// </remarks>
public sealed class PartitionReplica
{
    /// <summary>
    /// Gets the base queue name this partition belongs to.
    /// </summary>
    public required string QueueName { get; init; }

    /// <summary>
    /// Gets the zero-based partition index.
    /// </summary>
    public required int PartitionIndex { get; init; }

    /// <summary>
    /// Gets or sets the node ID of the primary for this partition.
    /// </summary>
    public Guid PrimaryNodeId { get; set; }

    /// <summary>
    /// Gets or sets the node ID of the replica for this partition.
    /// May be <see cref="Guid.Empty"/> if no replica is assigned (under-replicated).
    /// </summary>
    public Guid ReplicaNodeId { get; set; }

    /// <summary>
    /// Gets or sets the monotonically increasing epoch for this assignment.
    /// Incremented on every reassignment to detect stale primaries after a network partition.
    /// </summary>
    public long Epoch { get; set; }

    /// <summary>
    /// Gets a unique key for this partition (e.g., "orders:partition-2").
    /// </summary>
    public string PartitionKey =>
        Bielu.PersistentQueues.Partitioning.PartitionConstants.FormatPartitionQueueName(QueueName, PartitionIndex);

    /// <summary>
    /// Gets whether this partition is fully replicated (has both primary and replica).
    /// </summary>
    public bool IsFullyReplicated => PrimaryNodeId != Guid.Empty && ReplicaNodeId != Guid.Empty;

    public override string ToString() =>
        $"Partition({QueueName}:{PartitionIndex}, Primary={PrimaryNodeId:N}, Replica={ReplicaNodeId:N}, Epoch={Epoch})";
}
