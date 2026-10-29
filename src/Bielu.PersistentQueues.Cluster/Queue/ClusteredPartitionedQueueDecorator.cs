using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Bielu.PersistentQueues.Partitioning;
using Bielu.PersistentQueues.Serialization;
using Bielu.PersistentQueues.Storage;
using Microsoft.Extensions.Logging;

namespace Bielu.PersistentQueues.Cluster;

/// <summary>
/// A cluster-aware decorator over <see cref="IPartitionedQueue"/> that adds distributed
/// replication with a configurable replication factor.
/// </summary>
/// <remarks>
/// <para>
/// <see cref="ClusteredPartitionedQueueDecorator"/> wraps an existing <see cref="IPartitionedQueue"/> (which itself
/// wraps <see cref="IQueue"/>) and adds:
/// </para>
/// <list type="bullet">
///   <item>
///     <description><b>Write routing</b>: On enqueue, checks the partition assignment table.
///     If this node is the primary, writes locally and replicates to the replica.
///     If not, forwards to the correct primary via the existing <see cref="IQueue.Send(Message)"/> TCP path.</description>
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
public sealed class ClusteredPartitionedQueueDecorator : IPartitionedQueue, IAsyncDisposable
{
    private const int MaxSyncAttempts = 5;
    private static readonly TimeSpan SyncRetryBaseDelay = TimeSpan.FromSeconds(1);

    private readonly IPartitionedQueue _innerPartitionedQueue;
    private readonly IClusterCoordinator _coordinator;
    private readonly IPartitionAssignment _assignment;
    private readonly IReplicationProtocol _replication;
    private readonly ClusterConfiguration _config;
    private readonly ILogger _logger;
    private readonly IContentSerializer _contentSerializer;
    private readonly CancellationTokenSource _cts = new();
    private readonly object _reconcileLock = new();
    // Partitions this node took over as primary but has not finished syncing; their replica
    // copies must not be trimmed, since the primary may still be missing messages.
    private readonly ConcurrentDictionary<string, byte> _unsyncedPrimaryPartitions = new();
    private int _clusterStarted;
    private bool _disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="ClusteredPartitionedQueueDecorator"/> class.
    /// </summary>
    /// <param name="innerPartitionedQueue">The underlying partitioned queue.</param>
    /// <param name="coordinator">The cluster coordinator for membership tracking.</param>
    /// <param name="assignment">The partition assignment table.</param>
    /// <param name="replication">The replication protocol for primary → replica communication.</param>
    /// <param name="config">The cluster configuration.</param>
    /// <param name="logger">The logger instance.</param>
    /// <param name="contentSerializer">
    /// The serializer used by <see cref="Enqueue{T}"/>. Defaults to <see cref="JsonContentSerializer.Default"/>.
    /// </param>
    public ClusteredPartitionedQueueDecorator(
        IPartitionedQueue innerPartitionedQueue,
        IClusterCoordinator coordinator,
        IPartitionAssignment assignment,
        IReplicationProtocol replication,
        ClusterConfiguration config,
        ILogger<ClusteredPartitionedQueueDecorator> logger,
        IContentSerializer? contentSerializer = null)
    {
        _innerPartitionedQueue = innerPartitionedQueue ?? throw new ArgumentNullException(nameof(innerPartitionedQueue));
        _coordinator = coordinator ?? throw new ArgumentNullException(nameof(coordinator));
        _assignment = assignment ?? throw new ArgumentNullException(nameof(assignment));
        _replication = replication ?? throw new ArgumentNullException(nameof(replication));
        _config = config ?? throw new ArgumentNullException(nameof(config));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _contentSerializer = contentSerializer ?? JsonContentSerializer.Default;
        _config.Validate();

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
    /// <remarks>
    /// Starts the inner queue (receiver and sender) and then the cluster components
    /// (see <see cref="StartCluster"/>). When the inner queue is already started (for example by
    /// <c>AutoStart()</c>), call <see cref="StartCluster"/> instead.
    /// </remarks>
    public void Start()
    {
        _innerPartitionedQueue.Start();
        StartCluster();
    }

    /// <summary>
    /// Starts the replication listener, joins the cluster and assigns ownership of the
    /// partitioned queues that already exist in the store. Calling it more than once has no effect.
    /// </summary>
    /// <exception cref="System.Net.Sockets.SocketException">
    /// Thrown when the gossip or replication port cannot be bound.
    /// </exception>
    public void StartCluster()
    {
        if (Interlocked.Exchange(ref _clusterStarted, 1) == 1)
            return;

        InitializeAssignments();

        // Both calls bind their listener synchronously before the first await, so a port
        // conflict surfaces here instead of being lost in a discarded task.
        var listening = _replication.StartListeningAsync(_cts.Token);
        ThrowIfFaulted(listening);
        ObserveFaults(listening, "Replication listener");

        var joining = _coordinator.JoinAsync(_cts.Token);
        ThrowIfFaulted(joining);
        ObserveFaults(joining, "Cluster join");

        if (_config.ReplicationFactor > 1 && _config.ReplicaTrimInterval > TimeSpan.Zero)
            ObserveFaults(RunReplicaTrimLoopAsync(_cts.Token), "Replica trimming");

        _logger.LogInformation("Cluster started. Node {NodeId} listening on {Endpoint}",
            _coordinator.LocalNode.NodeId, _coordinator.LocalNode.QueueEndpoint);
    }

    private static void ThrowIfFaulted(Task task)
    {
        if (task.IsFaulted)
            task.GetAwaiter().GetResult();
    }

    private void ObserveFaults(Task task, string operation)
    {
        _ = task.ContinueWith(
            t => _logger.LogError(t.Exception, "{Operation} failed", operation),
            CancellationToken.None,
            TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);
    }

    /// <summary>
    /// Assigns ownership for every partitioned queue that exists in the store but has no
    /// assignment yet (e.g. queues created through <c>UsePartitioning(strategy, ("orders", 6))</c>).
    /// </summary>
    private void InitializeAssignments()
    {
        // The inner partitioned queue reports logical queue names
        foreach (var queueName in _innerPartitionedQueue.Queues)
        {
            var count = _innerPartitionedQueue.GetPartitionCount(queueName);
            if (count > 0 && _assignment.GetAssignments(queueName).Count == 0)
                AssignPartitions(queueName, count);
        }
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
        var ownedPartitions = FilterOwnedPartitions(queueName, partitions);

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
    /// <remarks>
    /// If the message's queue is partitioned, the partition is resolved with the configured
    /// <see cref="IPartitionStrategy"/> and the message is routed through the cluster
    /// (written locally and replicated when this node is the primary, forwarded otherwise).
    /// Non-partitioned queues are not cluster-managed and are enqueued locally.
    /// </remarks>
    public void Enqueue(Message message)
    {
        var queueName = message.QueueString;
        if (queueName != null)
        {
            var partitionCount = _innerPartitionedQueue.GetPartitionCount(queueName);
            if (partitionCount > 0)
            {
                var partition = _innerPartitionedQueue.PartitionStrategy.GetPartition(message, partitionCount);
                EnqueueToPartition(message, partition);
                return;
            }
        }

        _innerPartitionedQueue.Enqueue(message);
    }

    /// <inheritdoc />
    public void Enqueue<T>(
        T content,
        string? queueName = null,
        Dictionary<string, string>? headers = null,
        string? partitionKey = null)
    {
        var message = Message.Create(
            content,
            contentSerializer: _contentSerializer,
            queue: queueName,
            headers: headers,
            partitionKey: partitionKey);
        Enqueue(message);
    }

    /// <inheritdoc />
    public void EnqueueToPartition(Message message, int partition)
    {
        var queueName = message.QueueString
            ?? throw new InvalidOperationException("Message must have a queue name set.");
        var partitionAssignment = _assignment.GetAssignment(queueName, partition);

        if (partitionAssignment == null)
        {
            // No cluster assignment — fall back to local-only enqueue
            _innerPartitionedQueue.EnqueueToPartition(message, partition);
            return;
        }

        var partitionQueueName = PartitionConstants.FormatPartitionQueueName(queueName, partition);
        var localNodeId = _coordinator.LocalNode.NodeId;

        if (partitionAssignment.PrimaryNodeId == localNodeId)
        {
            // This node is the primary — write locally
            _innerPartitionedQueue.EnqueueToPartition(message, partition);
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

        // The node that accepts the write also replicates it, whether it stored the message as
        // primary or forwarded it: the primary's transport receiver stores forwarded messages
        // without going through this decorator, so it cannot replicate them itself.
        // The replica stores the message directly, so it must carry the partition sub-queue name.
        if (_config.ReplicationFactor > 1 && partitionAssignment.ReplicaNodeId != Guid.Empty)
        {
            var partitionMessage = message.WithQueue(partitionQueueName);
            if (partitionAssignment.ReplicaNodeId == localNodeId)
                Store.StoreIncoming(partitionMessage);
            else
                ReplicateToReplica(partitionAssignment, partitionMessage, partitionQueueName);
        }
    }

    private void ReplicateToReplica(PartitionReplica partitionAssignment, Message partitionMessage, string partitionQueueName)
    {
        var replicaNode = _coordinator.GetMembers()
            .FirstOrDefault(n => n.NodeId == partitionAssignment.ReplicaNodeId);

        if (replicaNode == null || replicaNode.State != NodeState.Active)
        {
            OnReplicationFailed(partitionAssignment,
                $"Replica node {partitionAssignment.ReplicaNodeId} for {partitionAssignment.PartitionKey} is not available.");
            return;
        }

        var replicaEndpoint = GetReplicationEndpoint(replicaNode);
        var replication = _replication.ReplicateAsync(
            replicaEndpoint, partitionQueueName, new[] { partitionMessage },
            partitionAssignment.Epoch, _cts.Token);

        if (_config.ReplicationMode == ReplicationMode.Synchronous)
        {
            // Intentional synchronous wait: IQueue.Enqueue/EnqueueToPartition are
            // synchronous by design (matching the existing IQueue contract). In sync
            // replication mode, we block until the replica acknowledges to guarantee
            // RF=2 durability before returning to the caller.
            if (!replication.GetAwaiter().GetResult())
            {
                OnReplicationFailed(partitionAssignment,
                    $"Replica node {partitionAssignment.ReplicaNodeId} did not acknowledge the write for {partitionAssignment.PartitionKey}.");
            }
        }
        // Asynchronous mode: fire-and-forget; ReplicateAsync never throws and logs its own failures.
    }

    private void OnReplicationFailed(PartitionReplica partitionAssignment, string reason)
    {
        if (_config.ReplicationMode == ReplicationMode.Synchronous)
        {
            throw new ClusterReplicationException(partitionAssignment.PartitionKey,
                reason + " The message was stored for the primary only (under-replicated).");
        }

        _logger.LogWarning("{Reason} Message stored for the primary only (under-replicated).", reason);
    }

    // ---- Partitioned queue pass-through ----

    /// <inheritdoc />
    public int GetPartitionCount(string queueName) => _innerPartitionedQueue.GetPartitionCount(queueName);

    /// <inheritdoc />
    public void CreatePartitionedQueue(string queueName, int partitionCount)
    {
        _innerPartitionedQueue.CreatePartitionedQueue(queueName, partitionCount);
        AssignPartitions(queueName, partitionCount);
    }

    /// <inheritdoc />
    public int ResolvePartition(Message message) =>
        _innerPartitionedQueue.ResolvePartition(message);

    /// <inheritdoc />
    public long GetPartitionMessageCount(string queueName, int partition) =>
        _innerPartitionedQueue.GetPartitionMessageCount(queueName, partition);

    /// <inheritdoc />
    /// <remarks>Only partitions for which this node is the primary are returned.</remarks>
    public int[] GetActivePartitions(string queueName) =>
        FilterOwnedPartitions(queueName, _innerPartitionedQueue.GetActivePartitions(queueName));

    /// <inheritdoc />
    /// <remarks>Only partitions for which this node is the primary are returned.</remarks>
    public int[] GetAvailablePartitions(string queueName) =>
        FilterOwnedPartitions(queueName, _innerPartitionedQueue.GetAvailablePartitions(queueName));

    /// <inheritdoc />
    /// <remarks>
    /// This only redistributes messages stored on the local node. Every node in the cluster
    /// must run the same operation while the queue is quiesced.
    /// </remarks>
    public void EnablePartitioning(string queueName, int partitionCount)
    {
        _innerPartitionedQueue.EnablePartitioning(queueName, partitionCount);
        AssignPartitions(queueName, partitionCount);
    }

    /// <inheritdoc />
    /// <remarks>
    /// This only affects the local node. Every node in the cluster must run the same
    /// operation while the queue is quiesced.
    /// </remarks>
    public void DisablePartitioning(string queueName) =>
        _innerPartitionedQueue.DisablePartitioning(queueName);

    /// <inheritdoc />
    /// <remarks>
    /// This only redistributes messages stored on the local node. Every node in the cluster
    /// must run the same operation while the queue is quiesced.
    /// </remarks>
    public void Repartition(string queueName, int newPartitionCount)
    {
        _innerPartitionedQueue.Repartition(queueName, newPartitionCount);
        AssignPartitions(queueName, newPartitionCount);
    }

    private void AssignPartitions(string queueName, int partitionCount)
    {
        var activeNodes = _coordinator.GetActiveMembers();
        if (activeNodes.Count > 0)
        {
            _assignment.AssignPartitions(queueName, partitionCount, activeNodes);
        }
    }

    private int[] FilterOwnedPartitions(string queueName, int[] partitions) =>
        partitions.Where(p => IsLocalPrimary(queueName, p)).ToArray();

    private bool IsLocalPrimary(string queueName, int partition)
    {
        var assignment = _assignment.GetAssignment(queueName, partition);
        return assignment == null || assignment.PrimaryNodeId == _coordinator.LocalNode.NodeId;
    }

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
    public void Send<T>(
        T content,
        string? destinationUri = null,
        string? queueName = null,
        Dictionary<string, string>? headers = null,
        DateTime? deliverBy = null,
        int? maxAttempts = null,
        string? partitionKey = null)
        => _innerPartitionedQueue.Send(content, destinationUri, queueName, headers, deliverBy, maxAttempts, partitionKey);

    /// <inheritdoc />
    public int RequeueDeadLetterMessages()
        => _innerPartitionedQueue.RequeueDeadLetterMessages();

    /// <inheritdoc />
    public int ClearDeadLetterQueue()
        => _innerPartitionedQueue.ClearDeadLetterQueue();

    // ---- Failover handling ----

    private void OnMembershipChanged(object? sender, ClusterMembershipChangedEventArgs e)
    {
        switch (e.ChangeType)
        {
            case ClusterMembershipChangeType.NodeDead:
            case ClusterMembershipChangeType.NodeLeft:
            case ClusterMembershipChangeType.NodeJoined:
            case ClusterMembershipChangeType.NodeRecovered:
                _logger.LogInformation("Membership change {ChangeType} for node {NodeId}. Reconciling partition ownership.",
                    e.ChangeType, e.Node.NodeId);
                ReconcileAssignments();
                break;
        }
    }

    /// <summary>
    /// Recomputes partition ownership for the current active membership and pulls data for
    /// every partition whose ownership moved to this node.
    /// </summary>
    /// <remarks>
    /// The assignment is a deterministic function of the active node set, so every node that
    /// sees the same membership converges on the same owners. When this node becomes the
    /// primary of a partition it copies the partition from the previous primary (or, when that
    /// node is gone, from the previous replica, which holds the replicated copy). When it becomes
    /// the replica it copies the partition from the new primary.
    /// </remarks>
    private void ReconcileAssignments()
    {
        lock (_reconcileLock)
        {
            var localNodeId = _coordinator.LocalNode.NodeId;
            var previous = SnapshotAssignments();

            _assignment.Rebalance(_coordinator.GetActiveMembers());

            foreach (var (key, current) in SnapshotAssignments())
            {
                if (!previous.TryGetValue(key, out var before))
                    continue;

                if (current.Primary == localNodeId && before.Primary != localNodeId)
                {
                    var source = FindActiveNode(before.Primary) ?? FindActiveNode(before.Replica, exclude: localNodeId);
                    if (source != null)
                    {
                        _unsyncedPrimaryPartitions[key] = 0;
                        _ = CatchUpSyncAsync(key, source);
                    }
                }
                else if (current.Replica == localNodeId && before.Replica != localNodeId)
                {
                    var source = FindActiveNode(current.Primary);
                    if (source != null)
                        _ = CatchUpSyncAsync(key, source);
                }
            }
        }
    }

    private Dictionary<string, (Guid Primary, Guid Replica)> SnapshotAssignments() =>
        _innerPartitionedQueue.Queues
            .SelectMany(_assignment.GetAssignments)
            .ToDictionary(a => a.PartitionKey, a => (a.PrimaryNodeId, a.ReplicaNodeId));

    private NodeInfo? FindActiveNode(Guid nodeId, Guid? exclude = null)
    {
        if (nodeId == Guid.Empty || nodeId == exclude)
            return null;

        return _coordinator.GetMembers()
            .FirstOrDefault(n => n.NodeId == nodeId && n.State == NodeState.Active);
    }

    private async Task CatchUpSyncAsync(string partitionQueueName, NodeInfo sourceNode)
    {
        try
        {
            var sourceEndpoint = GetReplicationEndpoint(sourceNode);

            _logger.LogInformation("Starting catch-up sync for {Partition} from {SourceNode}",
                partitionQueueName, sourceNode.NodeId);

            // This node may already hold some of these messages (e.g. it was the replica before
            // the change), so only store the ones that are missing locally.
            var localIds = Store.PersistedIncoming(partitionQueueName)
                .Select(m => m.Id)
                .ToHashSet();
            var synced = 0;
            MessageId? cursor = null;

            while (true)
            {
                var page = await SyncPageWithRetryAsync(sourceEndpoint, partitionQueueName, cursor).ConfigureAwait(false);
                if (page.Count == 0)
                    break;

                var missing = page.Where(m => localIds.Add(m.Id)).ToList();
                if (missing.Count > 0)
                {
                    Store.StoreIncoming(missing);
                    synced += missing.Count;
                }

                cursor = page[^1].Id;
            }

            _logger.LogInformation("Synced {Count} messages for {Partition} from {SourceNode}",
                synced, partitionQueueName, sourceNode.NodeId);

            // On failure the partition stays unsynced, which keeps its replica copies from being trimmed.
            _unsyncedPrimaryPartitions.TryRemove(partitionQueueName, out _);
        }
        catch (OperationCanceledException) when (_cts.IsCancellationRequested)
        {
            // Shutting down
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Catch-up sync failed for {Partition}; the partition may be missing messages " +
                "and its replica is no longer trimmed", partitionQueueName);
        }
    }

    private async Task<IReadOnlyList<Message>> SyncPageWithRetryAsync(IPEndPoint sourceEndpoint, string partitionQueueName,
        MessageId? cursor)
    {
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                return await _replication.SyncPartitionAsync(sourceEndpoint, partitionQueueName, cursor, _cts.Token)
                    .ConfigureAwait(false);
            }
            catch (ClusterReplicationException ex) when (attempt < MaxSyncAttempts)
            {
                var delay = TimeSpan.FromMilliseconds(SyncRetryBaseDelay.TotalMilliseconds * (1 << (attempt - 1)));
                _logger.LogWarning(ex, "Sync page for {Partition} failed (attempt {Attempt} of {MaxAttempts}); retrying in {Delay}",
                    partitionQueueName, attempt, MaxSyncAttempts, delay);
                await Task.Delay(delay, _cts.Token).ConfigureAwait(false);
            }
        }
    }

    // ---- Replica trimming ----

    private async Task RunReplicaTrimLoopAsync(CancellationToken cancellationToken)
    {
        using var timer = new PeriodicTimer(_config.ReplicaTrimInterval);
        try
        {
            while (await timer.WaitForNextTickAsync(cancellationToken).ConfigureAwait(false))
                await TrimReplicasAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Shutting down
        }
    }

    /// <summary>
    /// Removes replica copies of messages that are no longer stored on this node, for every
    /// partition this node owns as primary. A message leaves the primary when it is consumed
    /// (or moved), so without this the replica would keep every message ever written.
    /// </summary>
    private async Task TrimReplicasAsync(CancellationToken cancellationToken)
    {
        var sentBefore = DateTime.UtcNow - _config.ReplicaTrimGracePeriod;
        var localNodeId = _coordinator.LocalNode.NodeId;

        foreach (var assignment in _innerPartitionedQueue.Queues.SelectMany(_assignment.GetAssignments).ToList())
        {
            if (assignment.PrimaryNodeId != localNodeId)
                continue;

            var replica = FindActiveNode(assignment.ReplicaNodeId, exclude: localNodeId);
            if (replica == null)
                continue;

            try
            {
                await TrimReplicaAsync(assignment.PartitionKey, GetReplicationEndpoint(replica), sentBefore, cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (ClusterReplicationException ex)
            {
                _logger.LogWarning(ex, "Trimming replica of {Partition} failed; retrying on the next cycle", assignment.PartitionKey);
            }
        }
    }

    private async Task TrimReplicaAsync(string partitionQueueName, IPEndPoint replicaEndpoint, DateTime sentBefore,
        CancellationToken cancellationToken)
    {
        var trimmed = 0;
        MessageId? cursor = null;

        // Ownership can move while paging; only a fully synced primary knows which messages were consumed.
        while (CanTrimReplicaOf(partitionQueueName))
        {
            var page = await _replication.ListReplicaMessageIdsAsync(replicaEndpoint, partitionQueueName, sentBefore, cursor,
                cancellationToken).ConfigureAwait(false);
            if (page.Count == 0)
                break;

            var consumed = page.Where(id => Store.GetMessage(partitionQueueName, id) == null).ToList();
            if (consumed.Count > 0 && CanTrimReplicaOf(partitionQueueName))
            {
                if (!await _replication.TrimReplicaAsync(replicaEndpoint, partitionQueueName, consumed, cancellationToken)
                        .ConfigureAwait(false))
                {
                    throw new ClusterReplicationException(partitionQueueName,
                        $"Replica {replicaEndpoint} did not acknowledge trimming {partitionQueueName}.");
                }

                trimmed += consumed.Count;
            }

            cursor = page[^1];
        }

        if (trimmed > 0)
            _logger.LogDebug("Trimmed {Count} consumed messages from the replica of {Partition}", trimmed, partitionQueueName);
    }

    private bool CanTrimReplicaOf(string partitionQueueName)
    {
        if (_unsyncedPrimaryPartitions.ContainsKey(partitionQueueName) ||
            !PartitionConstants.TryParsePartitionQueueName(partitionQueueName, out var queueName, out var partition))
            return false;

        return _assignment.GetAssignment(queueName, partition)?.PrimaryNodeId == _coordinator.LocalNode.NodeId;
    }

    // ---- Disposal ----

    /// <summary>
    /// Gets the replication endpoint for a given node.
    /// The replication port is the queue port + gossip offset + 1.
    /// </summary>
    private IPEndPoint GetReplicationEndpoint(NodeInfo node) =>
        new(node.QueueEndpoint.Address, node.QueueEndpoint.Port + _config.GossipPortOffset + 1);

    /// <inheritdoc />
    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        _coordinator.MembershipChanged -= OnMembershipChanged;
        _cts.Cancel();

        if (_clusterStarted == 1)
            _coordinator.LeaveAsync().GetAwaiter().GetResult();

        _replication.Dispose();
        _coordinator.Dispose();
        _innerPartitionedQueue.Dispose();

        _cts.Dispose();
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        _coordinator.MembershipChanged -= OnMembershipChanged;
        await _cts.CancelAsync().ConfigureAwait(false);

        if (_clusterStarted == 1)
            await _coordinator.LeaveAsync().ConfigureAwait(false);

        await _replication.DisposeAsync().ConfigureAwait(false);
        await _coordinator.DisposeAsync().ConfigureAwait(false);
        await _innerPartitionedQueue.DisposeAsync().ConfigureAwait(false);

        _cts.Dispose();
    }
}
