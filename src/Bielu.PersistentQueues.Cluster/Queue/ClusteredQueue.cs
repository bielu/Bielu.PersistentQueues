using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Bielu.PersistentQueues.Partitioning;
using Bielu.PersistentQueues.Storage;
using Microsoft.Extensions.Logging;

namespace Bielu.PersistentQueues.Cluster;

/// <summary>
/// A cluster-aware decorator over <see cref="IPartitionedQueue"/> that adds distributed
/// replication with a configurable replication factor.
/// </summary>
/// <remarks>
/// <para>
/// <see cref="ClusteredQueue"/> wraps an existing <see cref="IPartitionedQueue"/> (which itself
/// wraps <see cref="IQueue"/>) and adds:
/// </para>
/// <list type="bullet">
///   <item>
///     <description><b>Write routing</b>: On enqueue, checks the partition assignment table.
///     If this node is the primary, writes locally and replicates to the replica.
///     If not, forwards to the correct primary via the existing <see cref="IQueue.Send"/> TCP path.</description>
///   </item>
///   <item>
///     <description><b>Replication</b>: After a local write, the primary replicates to the
///     assigned replica using <see cref="IReplicationProtocol"/>. In synchronous mode,
///     the write is not committed until the replica acknowledges.</description>
///   </item>
///   <item>
///     <description><b>Read filtering</b>: Consumers only receive messages from partitions
///     that this node owns as primary.</description>
///   </item>
///   <item>
///     <description><b>Failover</b>: Listens to <see cref="IClusterCoordinator.MembershipChanged"/>
///     events and triggers partition reassignment when nodes fail.</description>
///   </item>
/// </list>
/// </remarks>
public sealed class ClusteredQueue : IPartitionedQueue, IAsyncDisposable
{
    private readonly IPartitionedQueue _innerPartitionedQueue;
    private readonly IClusterCoordinator _coordinator;
    private readonly IPartitionAssignment _assignment;
    private readonly IReplicationProtocol _replication;
    private readonly ClusterConfiguration _config;
    private readonly ILogger _logger;
    private CancellationTokenSource? _cts;
    private bool _disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="ClusteredQueue"/> class.
    /// </summary>
    /// <param name="innerPartitionedQueue">The underlying partitioned queue.</param>
    /// <param name="coordinator">The cluster coordinator for membership tracking.</param>
    /// <param name="assignment">The partition assignment table.</param>
    /// <param name="replication">The replication protocol for primary → replica communication.</param>
    /// <param name="config">The cluster configuration.</param>
    /// <param name="logger">The logger instance.</param>
    public ClusteredQueue(
        IPartitionedQueue innerPartitionedQueue,
        IClusterCoordinator coordinator,
        IPartitionAssignment assignment,
        IReplicationProtocol replication,
        ClusterConfiguration config,
        ILogger logger)
    {
        _innerPartitionedQueue = innerPartitionedQueue ?? throw new ArgumentNullException(nameof(innerPartitionedQueue));
        _coordinator = coordinator ?? throw new ArgumentNullException(nameof(coordinator));
        _assignment = assignment ?? throw new ArgumentNullException(nameof(assignment));
        _replication = replication ?? throw new ArgumentNullException(nameof(replication));
        _config = config ?? throw new ArgumentNullException(nameof(config));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        // Subscribe to membership changes for automatic failover
        _coordinator.MembershipChanged += OnMembershipChanged;
    }

    // ---- IQueue delegation (passthrough to inner partitioned queue) ----

    /// <inheritdoc />
    public IMessageStore Store => _innerPartitionedQueue.Store;

    /// <inheritdoc />
    public string[] Queues => _innerPartitionedQueue.Queues;

    /// <inheritdoc />
    public IPEndPoint Endpoint => _innerPartitionedQueue.Endpoint;

    /// <inheritdoc />
    public IPartitionStrategy PartitionStrategy => _innerPartitionedQueue.PartitionStrategy;

    /// <inheritdoc />
    public void CreateQueue(string queueName) => _innerPartitionedQueue.CreateQueue(queueName);

    /// <inheritdoc />
    public void Start()
    {
        _cts = new CancellationTokenSource();

        // Start the inner queue (receiver, sender, etc.)
        _innerPartitionedQueue.Start();

        // Start replication listener in the background
        _ = _replication.StartListeningAsync(_cts.Token);

        // Join the cluster
        _ = _coordinator.JoinAsync(_cts.Token);

        _logger.LogInformation("ClusteredQueue started. Node {NodeId} listening on {Endpoint}",
            _coordinator.LocalNode.NodeId, _coordinator.LocalNode.QueueEndpoint);
    }

    // ---- Receive: only from partitions this node owns as primary ----

    /// <inheritdoc />
    public IAsyncEnumerable<IMessageContext> Receive(string queueName, int pollIntervalInMilliseconds = 200,
        CancellationToken cancellationToken = default)
    {
        // For non-partitioned receive, delegate directly
        return _innerPartitionedQueue.Receive(queueName, pollIntervalInMilliseconds, cancellationToken);
    }

    /// <inheritdoc />
    public IAsyncEnumerable<IBatchQueueContext> ReceiveBatch(string queueName,
        int maxMessages = 0, int batchTimeoutInMilliseconds = 0, int pollIntervalInMilliseconds = 200,
        CancellationToken cancellationToken = default)
    {
        return _innerPartitionedQueue.ReceiveBatch(queueName, maxMessages, batchTimeoutInMilliseconds,
            pollIntervalInMilliseconds, cancellationToken);
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<IMessageContext> ReceiveFromPartition(string queueName, int partition,
        int pollIntervalInMilliseconds = 200,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        // Only yield messages if this node is the primary for the partition
        var partitionAssignment = _assignment.GetAssignment(queueName, partition);
        if (partitionAssignment != null && partitionAssignment.PrimaryNodeId != _coordinator.LocalNode.NodeId)
        {
            _logger.LogWarning(
                "Node {NodeId} is not the primary for {QueueName}:partition-{Partition}. " +
                "Primary is {PrimaryId}. Consumer should connect to the primary node.",
                _coordinator.LocalNode.NodeId, queueName, partition, partitionAssignment.PrimaryNodeId);
            yield break;
        }

        await foreach (var ctx in _innerPartitionedQueue.ReceiveFromPartition(queueName, partition,
                            pollIntervalInMilliseconds, cancellationToken))
        {
            yield return ctx;
        }
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<IBatchQueueContext> ReceiveBatchFromPartition(string queueName, int partition,
        int maxMessages = 0, int batchTimeoutInMilliseconds = 0, int pollIntervalInMilliseconds = 200,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var partitionAssignment = _assignment.GetAssignment(queueName, partition);
        if (partitionAssignment != null && partitionAssignment.PrimaryNodeId != _coordinator.LocalNode.NodeId)
        {
            _logger.LogWarning(
                "Node {NodeId} is not the primary for {QueueName}:partition-{Partition}.",
                _coordinator.LocalNode.NodeId, queueName, partition);
            yield break;
        }

        await foreach (var ctx in _innerPartitionedQueue.ReceiveBatchFromPartition(queueName, partition,
                            maxMessages, batchTimeoutInMilliseconds, pollIntervalInMilliseconds, cancellationToken))
        {
            yield return ctx;
        }
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<IMessageContext> ReceiveFromPartitions(string queueName, int[] partitions,
        int pollIntervalInMilliseconds = 200,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        // Filter to only partitions this node is primary for
        var ownedPartitions = partitions
            .Where(p =>
            {
                var a = _assignment.GetAssignment(queueName, p);
                return a == null || a.PrimaryNodeId == _coordinator.LocalNode.NodeId;
            })
            .ToArray();

        if (ownedPartitions.Length == 0)
        {
            _logger.LogWarning("Node {NodeId} is not primary for any of the requested partitions of {QueueName}",
                _coordinator.LocalNode.NodeId, queueName);
            yield break;
        }

        await foreach (var ctx in _innerPartitionedQueue.ReceiveFromPartitions(queueName, ownedPartitions,
                            pollIntervalInMilliseconds, cancellationToken))
        {
            yield return ctx;
        }
    }

    // ---- Enqueue with replication ----

    /// <inheritdoc />
    public void EnqueueToPartition(Message message, string queueName)
    {
        var partitionCount = _innerPartitionedQueue.GetPartitionCount(queueName);
        if (partitionCount == 0)
            throw new InvalidOperationException($"No partitions found for queue '{queueName}'.");

        var partition = _innerPartitionedQueue.PartitionStrategy.GetPartition(message, partitionCount);
        EnqueueToPartition(message, queueName, partition);
    }

    /// <inheritdoc />
    public void EnqueueToPartition(Message message, string queueName, int partition)
    {
        var partitionAssignment = _assignment.GetAssignment(queueName, partition);

        if (partitionAssignment == null)
        {
            // No cluster assignment — fall back to local-only enqueue
            _innerPartitionedQueue.EnqueueToPartition(message, queueName, partition);
            return;
        }

        if (partitionAssignment.PrimaryNodeId == _coordinator.LocalNode.NodeId)
        {
            // This node is the primary — write locally
            _innerPartitionedQueue.EnqueueToPartition(message, queueName, partition);

            // Replicate to replica if assigned
            if (partitionAssignment.ReplicaNodeId != Guid.Empty)
            {
                var replicaNode = _coordinator.GetMembers()
                    .FirstOrDefault(n => n.NodeId == partitionAssignment.ReplicaNodeId);

                if (replicaNode != null && replicaNode.State == NodeState.Active)
                {
                    var partitionQueueName = PartitionConstants.FormatPartitionQueueName(queueName, partition);
                    var replicaEndpoint = new IPEndPoint(
                        replicaNode.QueueEndpoint.Address,
                        replicaNode.QueueEndpoint.Port + _config.GossipPortOffset + 1);

                    if (_config.ReplicationMode == ReplicationMode.Synchronous)
                    {
                        // Block until replica acknowledges
                        var acked = _replication.ReplicateAsync(
                            replicaEndpoint, partitionQueueName, new[] { message },
                            partitionAssignment.Epoch).GetAwaiter().GetResult();

                        if (!acked)
                        {
                            _logger.LogWarning(
                                "Synchronous replication failed for {Partition}. Message stored locally only.",
                                partitionAssignment.PartitionKey);
                        }
                    }
                    else
                    {
                        // Fire-and-forget async replication
                        _ = _replication.ReplicateAsync(
                            replicaEndpoint, partitionQueueName, new[] { message },
                            partitionAssignment.Epoch);
                    }
                }
                else
                {
                    _logger.LogWarning("Replica node {ReplicaNodeId} for {Partition} is not available. " +
                                       "Message stored on primary only (under-replicated).",
                        partitionAssignment.ReplicaNodeId, partitionAssignment.PartitionKey);
                }
            }
        }
        else
        {
            // This node is NOT the primary — forward to the correct primary
            var primaryNode = _coordinator.GetMembers()
                .FirstOrDefault(n => n.NodeId == partitionAssignment.PrimaryNodeId);

            if (primaryNode == null || primaryNode.State != NodeState.Active)
            {
                throw new InvalidOperationException(
                    $"Primary node {partitionAssignment.PrimaryNodeId} for {partitionAssignment.PartitionKey} is not available.");
            }

            // Use the existing Send() mechanism to forward to the primary
            var partitionQueueName = PartitionConstants.FormatPartitionQueueName(queueName, partition);
            var destinationUri = $"lq.tcp://{primaryNode.QueueEndpoint}";

            var forwardedMessage = new Message(
                message.Id,
                message.Data,
                partitionQueueName.AsMemory(),
                message.SentAt,
                message.SubQueue,
                destinationUri.AsMemory(),
                message.DeliverBy,
                message.MaxAttempts,
                message.Headers,
                partitionKey: message.PartitionKey);

            _innerPartitionedQueue.Send(forwardedMessage);
        }
    }

    // ---- Partitioned queue pass-through ----

    /// <inheritdoc />
    public int GetPartitionCount(string queueName) => _innerPartitionedQueue.GetPartitionCount(queueName);

    /// <inheritdoc />
    public void CreatePartitionedQueue(string queueName, int partitionCount)
    {
        _innerPartitionedQueue.CreatePartitionedQueue(queueName, partitionCount);

        // Assign partitions across the cluster
        var activeNodes = _coordinator.GetActiveMembers();
        if (activeNodes.Count > 0)
        {
            _assignment.AssignPartitions(queueName, partitionCount, activeNodes);
        }
    }

    /// <inheritdoc />
    public int ResolvePartition(Message message, string queueName) =>
        _innerPartitionedQueue.ResolvePartition(message, queueName);

    // ---- Simple IQueue delegation ----

    /// <inheritdoc />
    public void ReceiveLater(Message message, TimeSpan timeSpan)
        => _innerPartitionedQueue.ReceiveLater(message, timeSpan);

    /// <inheritdoc />
    public void ReceiveLater(Message message, DateTimeOffset time)
        => _innerPartitionedQueue.ReceiveLater(message, time);

    /// <inheritdoc />
    public void MoveToQueue(string queueName, Message message)
        => _innerPartitionedQueue.MoveToQueue(queueName, message);

    /// <inheritdoc />
    public void Send(params Message[] messages)
        => _innerPartitionedQueue.Send(messages);

    /// <inheritdoc />
    public void Send(Message message)
        => _innerPartitionedQueue.Send(message);

    /// <inheritdoc />
    public void Enqueue(Message message)
        => _innerPartitionedQueue.Enqueue(message);

    // ---- Failover handling ----

    private void OnMembershipChanged(object? sender, ClusterMembershipChangedEventArgs e)
    {
        switch (e.ChangeType)
        {
            case ClusterMembershipChangeType.NodeDead:
                HandleNodeFailure(e.Node);
                break;

            case ClusterMembershipChangeType.NodeJoined:
            case ClusterMembershipChangeType.NodeRecovered:
                HandleNodeJoin(e.Node);
                break;
        }
    }

    private void HandleNodeFailure(NodeInfo failedNode)
    {
        _logger.LogWarning("Node {NodeId} failed. Checking for partitions to promote.", failedNode.NodeId);

        // Find all partitions where the failed node was the primary
        var primaryPartitions = _assignment.GetPrimaryPartitions(failedNode.NodeId);
        foreach (var partition in primaryPartitions)
        {
            // If this node is the replica, promote ourselves
            if (partition.ReplicaNodeId == _coordinator.LocalNode.NodeId)
            {
                _assignment.PromoteReplica(partition.QueueName, partition.PartitionIndex);
                _logger.LogInformation(
                    "Promoted self to primary for {Partition} after failure of {FailedNode}",
                    partition.PartitionKey, failedNode.NodeId);
            }
        }

        // Rebalance to assign new replicas for under-replicated partitions
        _assignment.Rebalance(_coordinator.GetActiveMembers());
    }

    private void HandleNodeJoin(NodeInfo newNode)
    {
        _logger.LogInformation("Node {NodeId} joined. Triggering rebalance.", newNode.NodeId);

        // Rebalance to spread partitions across the new set of nodes
        _assignment.Rebalance(_coordinator.GetActiveMembers());

        // Trigger catch-up sync for any partitions this node is now a replica for
        var replicaPartitions = _assignment.GetReplicaPartitions(_coordinator.LocalNode.NodeId);
        foreach (var partition in replicaPartitions)
        {
            if (partition.PrimaryNodeId != Guid.Empty)
            {
                var primaryNode = _coordinator.GetMembers()
                    .FirstOrDefault(n => n.NodeId == partition.PrimaryNodeId);

                if (primaryNode != null)
                {
                    _ = CatchUpSyncAsync(partition, primaryNode);
                }
            }
        }
    }

    private async Task CatchUpSyncAsync(PartitionReplica partition, NodeInfo primaryNode)
    {
        try
        {
            var sourceEndpoint = new IPEndPoint(
                primaryNode.QueueEndpoint.Address,
                primaryNode.QueueEndpoint.Port + _config.GossipPortOffset + 1);

            _logger.LogInformation("Starting catch-up sync for {Partition} from {PrimaryNode}",
                partition.PartitionKey, primaryNode.NodeId);

            var messages = await _replication.SyncPartitionAsync(
                sourceEndpoint, partition.PartitionKey, afterMessageId: null,
                _cts?.Token ?? CancellationToken.None).ConfigureAwait(false);

            if (messages.Count > 0)
            {
                Store.StoreIncoming(messages);
                _logger.LogInformation("Synced {Count} messages for {Partition} from {PrimaryNode}",
                    messages.Count, partition.PartitionKey, primaryNode.NodeId);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Catch-up sync failed for {Partition}", partition.PartitionKey);
        }
    }

    // ---- Disposal ----

    /// <inheritdoc />
    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        _coordinator.MembershipChanged -= OnMembershipChanged;
        _cts?.Cancel();

        _coordinator.LeaveAsync().GetAwaiter().GetResult();

        _replication.Dispose();
        _coordinator.Dispose();
        _innerPartitionedQueue.Dispose();

        _cts?.Dispose();
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        _coordinator.MembershipChanged -= OnMembershipChanged;
        if (_cts != null) await _cts.CancelAsync().ConfigureAwait(false);

        await _coordinator.LeaveAsync().ConfigureAwait(false);

        await _replication.DisposeAsync().ConfigureAwait(false);
        await _coordinator.DisposeAsync().ConfigureAwait(false);
        await _innerPartitionedQueue.DisposeAsync().ConfigureAwait(false);

        _cts?.Dispose();
    }
}
