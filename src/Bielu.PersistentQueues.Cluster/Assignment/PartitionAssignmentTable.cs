using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Logging;

namespace Bielu.PersistentQueues.Cluster;

/// <summary>
/// In-memory partition assignment table that distributes partitions evenly
/// across cluster nodes using a round-robin approach with minimal movement on rebalance.
/// </summary>
/// <remarks>
/// <para>
/// On initial assignment, partitions are distributed evenly: for each partition,
/// the primary is chosen round-robin across active nodes, and the replica is
/// assigned to a different node.
/// </para>
/// <para>
/// On rebalance (node join/leave), only partitions that were on a failed node are moved.
/// This minimizes data movement — existing healthy assignments are preserved.
/// </para>
/// </remarks>
public sealed class PartitionAssignmentTable : IPartitionAssignment
{
    private readonly ConcurrentDictionary<string, List<PartitionReplica>> _assignments = new();
    private readonly ILogger _logger;
    private readonly object _rebalanceLock = new();

    /// <inheritdoc />
    public event EventHandler<PartitionAssignmentChangedEventArgs>? AssignmentChanged;

    /// <summary>
    /// Initializes a new instance of the <see cref="PartitionAssignmentTable"/> class.
    /// </summary>
    /// <param name="logger">The logger instance.</param>
    public PartitionAssignmentTable(ILogger logger)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
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
            var partitions = new List<PartitionReplica>(partitionCount);

            for (var i = 0; i < partitionCount; i++)
            {
                var primaryIdx = i % activeNodes.Count;
                var replicaIdx = activeNodes.Count > 1
                    ? (i + 1) % activeNodes.Count
                    : -1; // No replica possible with single node

                var partition = new PartitionReplica
                {
                    QueueName = queueName,
                    PartitionIndex = i,
                    PrimaryNodeId = activeNodes[primaryIdx].NodeId,
                    ReplicaNodeId = replicaIdx >= 0 ? activeNodes[replicaIdx].NodeId : Guid.Empty,
                    Epoch = 1
                };
                partitions.Add(partition);
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
            var activeNodeIds = new HashSet<Guid>(activeNodes.Select(n => n.NodeId));
            var changed = new List<PartitionReplica>();

            foreach (var (queueName, partitions) in _assignments)
            {
                for (var i = 0; i < partitions.Count; i++)
                {
                    var partition = partitions[i];
                    var modified = false;

                    // Check if primary node is still active
                    if (!activeNodeIds.Contains(partition.PrimaryNodeId))
                    {
                        if (partition.ReplicaNodeId != Guid.Empty &&
                            activeNodeIds.Contains(partition.ReplicaNodeId))
                        {
                            // Promote replica to primary
                            _logger.LogInformation(
                                "Promoting replica {ReplicaNodeId} to primary for {Partition}",
                                partition.ReplicaNodeId, partition.PartitionKey);

                            partition.PrimaryNodeId = partition.ReplicaNodeId;
                            partition.ReplicaNodeId = Guid.Empty;
                        }
                        else
                        {
                            // Assign new primary from active nodes (least loaded)
                            var newPrimary = GetLeastLoadedNode(activeNodes, activeNodeIds, partition.ReplicaNodeId);
                            partition.PrimaryNodeId = newPrimary;
                        }
                        partition.Epoch++;
                        modified = true;
                    }

                    // Check if replica node is still active or is unassigned
                    if (partition.ReplicaNodeId == Guid.Empty ||
                        !activeNodeIds.Contains(partition.ReplicaNodeId))
                    {
                        if (activeNodes.Count > 1)
                        {
                            // Assign a new replica (different from primary, least loaded)
                            var newReplica = GetLeastLoadedNode(activeNodes, activeNodeIds, partition.PrimaryNodeId);
                            if (newReplica != partition.PrimaryNodeId)
                            {
                                partition.ReplicaNodeId = newReplica;
                                modified = true;
                            }
                        }
                        else
                        {
                            partition.ReplicaNodeId = Guid.Empty;
                            modified = true;
                        }
                    }

                    if (modified)
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

    /// <summary>
    /// Selects the active node with the fewest assigned partitions (primary + replica),
    /// excluding a specific node ID.
    /// </summary>
    private Guid GetLeastLoadedNode(IReadOnlyList<NodeInfo> activeNodes, HashSet<Guid> activeNodeIds, Guid excludeNodeId)
    {
        var loadCounts = new Dictionary<Guid, int>();
        foreach (var node in activeNodes)
        {
            loadCounts[node.NodeId] = 0;
        }

        foreach (var partitions in _assignments.Values)
        {
            foreach (var p in partitions)
            {
                if (loadCounts.ContainsKey(p.PrimaryNodeId))
                    loadCounts[p.PrimaryNodeId]++;
                if (loadCounts.ContainsKey(p.ReplicaNodeId))
                    loadCounts[p.ReplicaNodeId]++;
            }
        }

        return loadCounts
            .Where(kv => kv.Key != excludeNodeId && activeNodeIds.Contains(kv.Key))
            .OrderBy(kv => kv.Value)
            .Select(kv => kv.Key)
            .FirstOrDefault(activeNodes[0].NodeId);
    }
}
