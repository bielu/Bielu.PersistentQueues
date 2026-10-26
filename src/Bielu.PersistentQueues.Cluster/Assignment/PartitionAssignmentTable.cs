using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Logging;

namespace Bielu.PersistentQueues.Cluster;

/// <summary>
/// In-memory partition assignment table that distributes partitions evenly
/// across cluster nodes using a deterministic round-robin approach.
/// </summary>
/// <remarks>
/// <para>
/// Assignments are a pure function of the active node set: nodes are ordered by
/// <see cref="NodeInfo.NodeId"/>, partition <c>i</c> gets the node at <c>i % n</c> as primary
/// and the node at <c>(i + 1) % n</c> as replica. Every node that sees the same membership
/// therefore computes the same table, without any extra coordination.
/// </para>
/// <para>
/// <see cref="Rebalance"/> recomputes the table for the given active nodes and increments the
/// epoch of every partition whose primary changed. Callers are responsible for moving data
/// to the new owners (see <see cref="ClusteredPartitionedQueueDecorator"/>).
/// </para>
/// </remarks>
public sealed class PartitionAssignmentTable : IPartitionAssignment
{
    private readonly ConcurrentDictionary<string, List<PartitionReplica>> _assignments = new();
    private readonly ILogger _logger;
    private readonly int _replicationFactor;
    private readonly object _rebalanceLock = new();

    /// <inheritdoc />
    public event EventHandler<PartitionAssignmentChangedEventArgs>? AssignmentChanged;

    /// <summary>
    /// Initializes a new instance of the <see cref="PartitionAssignmentTable"/> class.
    /// </summary>
    /// <param name="logger">The logger instance.</param>
    /// <param name="replicationFactor">
    /// The number of copies of each partition: <c>1</c> (primary only) or <c>2</c> (primary and replica).
    /// </param>
    public PartitionAssignmentTable(ILogger<PartitionAssignmentTable> logger, int replicationFactor = 2)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        ClusterConfiguration.ValidateReplicationFactor(replicationFactor);
        _replicationFactor = replicationFactor;
    }

    /// <inheritdoc />
    public PartitionReplica? GetAssignment(string queueName, int partitionIndex)
    {
        if (_assignments.TryGetValue(queueName, out var partitions))
        {
            return partitions.FirstOrDefault(p => p.PartitionIndex == partitionIndex);
        }
        return null;
    }

    /// <inheritdoc />
    public IReadOnlyList<PartitionReplica> GetAssignments(string queueName)
    {
        if (_assignments.TryGetValue(queueName, out var partitions))
            return partitions.AsReadOnly();
        return Array.Empty<PartitionReplica>();
    }

    /// <inheritdoc />
    public IReadOnlyList<PartitionReplica> GetPrimaryPartitions(Guid nodeId)
    {
        return _assignments.Values
            .SelectMany(p => p)
            .Where(p => p.PrimaryNodeId == nodeId)
            .ToList()
            .AsReadOnly();
    }

    /// <inheritdoc />
    public IReadOnlyList<PartitionReplica> GetReplicaPartitions(Guid nodeId)
    {
        return _assignments.Values
            .SelectMany(p => p)
            .Where(p => p.ReplicaNodeId == nodeId)
            .ToList()
            .AsReadOnly();
    }

    /// <inheritdoc />
    public void AssignPartitions(string queueName, int partitionCount, IReadOnlyList<NodeInfo> activeNodes)
    {
        if (activeNodes.Count == 0)
            throw new InvalidOperationException("Cannot assign partitions with no active nodes.");

        lock (_rebalanceLock)
        {
            var orderedNodes = OrderNodes(activeNodes);
            var partitions = new List<PartitionReplica>(partitionCount);

            for (var i = 0; i < partitionCount; i++)
            {
                var (primary, replica) = ComputeOwners(i, orderedNodes);
                partitions.Add(new PartitionReplica
                {
                    QueueName = queueName,
                    PartitionIndex = i,
                    PrimaryNodeId = primary,
                    ReplicaNodeId = replica,
                    Epoch = 1
                });
            }

            _assignments[queueName] = partitions;

            _logger.LogInformation("Assigned {PartitionCount} partitions for queue '{QueueName}' across {NodeCount} nodes",
                partitionCount, queueName, activeNodes.Count);

            AssignmentChanged?.Invoke(this, new PartitionAssignmentChangedEventArgs
            {
                ChangedPartitions = partitions.AsReadOnly()
            });
        }
    }

    /// <inheritdoc />
    public void Rebalance(IReadOnlyList<NodeInfo> activeNodes)
    {
        if (activeNodes.Count == 0) return;

        lock (_rebalanceLock)
        {
            var orderedNodes = OrderNodes(activeNodes);
            var changed = new List<PartitionReplica>();

            foreach (var partitions in _assignments.Values)
            {
                foreach (var partition in partitions)
                {
                    var (primary, replica) = ComputeOwners(partition.PartitionIndex, orderedNodes);
                    if (primary == partition.PrimaryNodeId && replica == partition.ReplicaNodeId)
                        continue;

                    if (primary != partition.PrimaryNodeId)
                    {
                        _logger.LogInformation("Moving primary for {Partition}: {OldPrimary} → {NewPrimary}",
                            partition.PartitionKey, partition.PrimaryNodeId, primary);
                        partition.Epoch++;
                    }

                    partition.PrimaryNodeId = primary;
                    partition.ReplicaNodeId = replica;
                    changed.Add(partition);
                }
            }

            if (changed.Count > 0)
            {
                _logger.LogInformation("Rebalanced {Count} partition assignments", changed.Count);
                AssignmentChanged?.Invoke(this, new PartitionAssignmentChangedEventArgs
                {
                    ChangedPartitions = changed.AsReadOnly()
                });
            }
        }
    }

    /// <inheritdoc />
    public PartitionReplica? PromoteReplica(string queueName, int partitionIndex)
    {
        lock (_rebalanceLock)
        {
            var partition = GetAssignment(queueName, partitionIndex);
            if (partition == null) return null;

            if (partition.ReplicaNodeId == Guid.Empty)
            {
                _logger.LogWarning("Cannot promote replica for {Partition} — no replica assigned", partition.PartitionKey);
                return null;
            }

            var oldPrimary = partition.PrimaryNodeId;
            partition.PrimaryNodeId = partition.ReplicaNodeId;
            partition.ReplicaNodeId = Guid.Empty; // Replica is now unassigned until a new node is found
            partition.Epoch++;

            _logger.LogInformation(
                "Promoted replica to primary for {Partition}: {OldPrimary} → {NewPrimary} (epoch {Epoch})",
                partition.PartitionKey, oldPrimary, partition.PrimaryNodeId, partition.Epoch);

            AssignmentChanged?.Invoke(this, new PartitionAssignmentChangedEventArgs
            {
                ChangedPartitions = new[] { partition }
            });

            return partition;
        }
    }

    private static List<NodeInfo> OrderNodes(IReadOnlyList<NodeInfo> nodes) =>
        nodes.DistinctBy(n => n.NodeId).OrderBy(n => n.NodeId).ToList();

    private (Guid Primary, Guid Replica) ComputeOwners(int partitionIndex, List<NodeInfo> orderedNodes)
    {
        var primary = orderedNodes[partitionIndex % orderedNodes.Count].NodeId;
        var replica = _replicationFactor > 1 && orderedNodes.Count > 1
            ? orderedNodes[(partitionIndex + 1) % orderedNodes.Count].NodeId
            : Guid.Empty;
        return (primary, replica);
    }
}
