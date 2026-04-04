using System;
using System.Collections.Generic;

namespace Bielu.PersistentQueues.Cluster;

/// <summary>
/// Maps partitions to their primary and replica nodes.
/// </summary>
/// <remarks>
/// The partition assignment table is the cluster-wide truth about which node owns
/// which partition. Implementations are responsible for maintaining consistency
/// across all cluster members (e.g., via gossip, external coordination, etc.).
/// </remarks>
public interface IPartitionAssignment
{
    /// <summary>
    /// Gets the assignment for a specific partition.
    /// </summary>
    /// <param name="queueName">The base queue name.</param>
    /// <param name="partitionIndex">The zero-based partition index.</param>
    /// <returns>The partition assignment, or null if not found.</returns>
    PartitionReplica? GetAssignment(string queueName, int partitionIndex);

    /// <summary>
    /// Gets all assignments for a given queue.
    /// </summary>
    /// <param name="queueName">The base queue name.</param>
    /// <returns>All partition assignments for the specified queue.</returns>
    IReadOnlyList<PartitionReplica> GetAssignments(string queueName);

    /// <summary>
    /// Gets all partitions assigned to a specific node as primary.
    /// </summary>
    /// <param name="nodeId">The node identifier.</param>
    /// <returns>All partitions where this node is the primary.</returns>
    IReadOnlyList<PartitionReplica> GetPrimaryPartitions(Guid nodeId);

    /// <summary>
    /// Gets all partitions assigned to a specific node as replica.
    /// </summary>
    /// <param name="nodeId">The node identifier.</param>
    /// <returns>All partitions where this node is the replica.</returns>
    IReadOnlyList<PartitionReplica> GetReplicaPartitions(Guid nodeId);

    /// <summary>
    /// Assigns partitions for a queue across available nodes.
    /// This is typically called when a partitioned queue is created or when
    /// the cluster membership changes and rebalancing is needed.
    /// </summary>
    /// <param name="queueName">The base queue name.</param>
    /// <param name="partitionCount">The number of partitions.</param>
    /// <param name="activeNodes">The currently active nodes in the cluster.</param>
    void AssignPartitions(string queueName, int partitionCount, IReadOnlyList<NodeInfo> activeNodes);

    /// <summary>
    /// Rebalances partition assignments after a membership change.
    /// Attempts to minimize partition movement while maintaining even distribution.
    /// </summary>
    /// <param name="activeNodes">The currently active nodes.</param>
    void Rebalance(IReadOnlyList<NodeInfo> activeNodes);

    /// <summary>
    /// Promotes the replica of a partition to primary (used during failover).
    /// </summary>
    /// <param name="queueName">The base queue name.</param>
    /// <param name="partitionIndex">The partition index.</param>
    /// <returns>The updated assignment, or null if the partition was not found.</returns>
    PartitionReplica? PromoteReplica(string queueName, int partitionIndex);

    /// <summary>
    /// Raised when partition assignments change (after rebalance, promotion, etc.).
    /// </summary>
    event EventHandler<PartitionAssignmentChangedEventArgs> AssignmentChanged;
}

/// <summary>
/// Provides data for partition assignment change events.
/// </summary>
public sealed class PartitionAssignmentChangedEventArgs : EventArgs
{
    /// <summary>
    /// Gets the partitions whose assignments changed.
    /// </summary>
    public required IReadOnlyList<PartitionReplica> ChangedPartitions { get; init; }
}
