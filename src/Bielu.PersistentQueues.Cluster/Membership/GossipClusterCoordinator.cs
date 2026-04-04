using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Bielu.PersistentQueues.Cluster;

/// <summary>
/// A gossip-based cluster coordinator that uses periodic heartbeats over TCP
/// to maintain a consistent view of cluster membership.
/// </summary>
/// <remarks>
/// <para>
/// Implements a simplified SWIM-style protocol:
/// <list type="number">
///   <item>Each node periodically sends heartbeats to a random subset of known peers.</item>
///   <item>Heartbeats include the sender's full membership table (gossip-style dissemination).</item>
///   <item>If a node misses heartbeats beyond a configurable threshold, it is marked suspected, then dead.</item>
///   <item>When a node joins, it contacts seed nodes to discover the cluster state.</item>
/// </list>
/// </para>
/// <para>
/// No external infrastructure (etcd, Consul, ZooKeeper) is required.
/// </para>
/// </remarks>
public sealed class GossipClusterCoordinator : IClusterCoordinator
{
    private readonly ClusterConfiguration _config;
    private readonly ILogger _logger;
    private readonly ConcurrentDictionary<Guid, NodeInfo> _members = new();
    private TcpListener? _listener;
    private CancellationTokenSource? _cts;
    private Task? _heartbeatTask;
    private Task? _listenerTask;
    private Task? _failureDetectionTask;
    private bool _disposed;

    /// <inheritdoc />
    public NodeInfo LocalNode { get; }

    /// <inheritdoc />
    public event EventHandler<ClusterMembershipChangedEventArgs>? MembershipChanged;

    /// <summary>
    /// Initializes a new instance of the <see cref="GossipClusterCoordinator"/> class.
    /// </summary>
    /// <param name="config">The cluster configuration.</param>
    /// <param name="queueEndpoint">The local queue endpoint.</param>
    /// <param name="logger">The logger instance.</param>
    public GossipClusterCoordinator(ClusterConfiguration config, IPEndPoint queueEndpoint, ILogger logger)
    {
        _config = config ?? throw new ArgumentNullException(nameof(config));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        var gossipEndpoint = new IPEndPoint(queueEndpoint.Address, queueEndpoint.Port + config.GossipPortOffset);

        LocalNode = new NodeInfo
        {
            NodeId = config.NodeId,
            QueueEndpoint = queueEndpoint,
            GossipEndpoint = gossipEndpoint,
            State = NodeState.Active,
            LastSeen = DateTime.UtcNow,
            Generation = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
        };

        _members[LocalNode.NodeId] = LocalNode;
    }

    /// <inheritdoc />
    public IReadOnlyList<NodeInfo> GetMembers() =>
        _members.Values.ToList().AsReadOnly();

    /// <inheritdoc />
    public IReadOnlyList<NodeInfo> GetActiveMembers() =>
        _members.Values.Where(n => n.State == NodeState.Active).ToList().AsReadOnly();

    /// <inheritdoc />
    public async Task JoinAsync(CancellationToken cancellationToken = default)
    {
        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        // Start listening for gossip messages
        _listener = new TcpListener(LocalNode.GossipEndpoint);
        _listener.Start();
        _listenerTask = AcceptGossipConnectionsAsync(_cts.Token);

        // Contact seed nodes to discover cluster
        foreach (var seed in _config.SeedNodes)
        {
            if (seed.Equals(LocalNode.GossipEndpoint))
                continue;

            try
            {
                await SendGossipAsync(seed, _cts.Token).ConfigureAwait(false);
                _logger.LogInformation("Successfully contacted seed node at {Endpoint}", seed);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to contact seed node at {Endpoint}", seed);
            }
        }

        // Start periodic heartbeat
        _heartbeatTask = RunHeartbeatLoopAsync(_cts.Token);

        // Start failure detection
        _failureDetectionTask = RunFailureDetectionAsync(_cts.Token);

        _logger.LogInformation("Node {NodeId} joined cluster via gossip on {Endpoint}",
            LocalNode.NodeId, LocalNode.GossipEndpoint);
    }

    /// <inheritdoc />
    public async Task LeaveAsync(CancellationToken cancellationToken = default)
    {
        LocalNode.State = NodeState.Draining;

        // Notify peers about departure
        var peers = _members.Values
            .Where(n => n.NodeId != LocalNode.NodeId && n.State == NodeState.Active)
            .ToList();

        foreach (var peer in peers)
        {
            try
            {
                await SendGossipAsync(peer.GossipEndpoint, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to notify peer {NodeId} of departure", peer.NodeId);
            }
        }

        _logger.LogInformation("Node {NodeId} leaving cluster", LocalNode.NodeId);
    }

    private async Task AcceptGossipConnectionsAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested && !_disposed)
        {
            try
            {
                var socket = await _listener!.AcceptSocketAsync(cancellationToken).ConfigureAwait(false);
                _ = HandleGossipConnectionAsync(socket, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error accepting gossip connection");
            }
        }
    }

    private async Task HandleGossipConnectionAsync(Socket socket, CancellationToken cancellationToken)
    {
        try
        {
            await using var stream = new NetworkStream(socket, ownsSocket: true);
            using var reader = new StreamReader(stream, Encoding.UTF8);
            var json = await reader.ReadToEndAsync(cancellationToken).ConfigureAwait(false);
            var gossipData = JsonSerializer.Deserialize<GossipMessage>(json);

            if (gossipData?.Members != null)
            {
                MergeGossipState(gossipData.Members);
            }
        }
        catch (OperationCanceledException)
        {
            // Expected during shutdown
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error handling gossip connection");
        }
    }

    private async Task SendGossipAsync(IPEndPoint target, CancellationToken cancellationToken)
    {
        using var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        socket.NoDelay = true;

        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutCts.CancelAfter(TimeSpan.FromSeconds(5));

        await socket.ConnectAsync(target, timeoutCts.Token).ConfigureAwait(false);
        await using var stream = new NetworkStream(socket, ownsSocket: false);

        // Update local node's LastSeen before gossiping
        LocalNode.LastSeen = DateTime.UtcNow;

        var gossipMessage = new GossipMessage
        {
            SenderId = LocalNode.NodeId,
            Members = _members.Values.Select(n => new GossipNodeEntry
            {
                NodeId = n.NodeId,
                QueueEndpointAddress = n.QueueEndpoint.Address.ToString(),
                QueueEndpointPort = n.QueueEndpoint.Port,
                GossipEndpointAddress = n.GossipEndpoint.Address.ToString(),
                GossipEndpointPort = n.GossipEndpoint.Port,
                State = n.State,
                LastSeen = n.LastSeen,
                Generation = n.Generation
            }).ToList()
        };

        var json = JsonSerializer.Serialize(gossipMessage);
        var bytes = Encoding.UTF8.GetBytes(json);
        await stream.WriteAsync(bytes, cancellationToken).ConfigureAwait(false);
    }

    private void MergeGossipState(List<GossipNodeEntry> remoteMembers)
    {
        foreach (var remote in remoteMembers)
        {
            var endpoint = new IPEndPoint(IPAddress.Parse(remote.QueueEndpointAddress), remote.QueueEndpointPort);
            var gossipEndpoint = new IPEndPoint(IPAddress.Parse(remote.GossipEndpointAddress), remote.GossipEndpointPort);

            if (_members.TryGetValue(remote.NodeId, out var existing))
            {
                // Use generation + LastSeen for conflict resolution (higher wins)
                if (remote.Generation > existing.Generation ||
                    (remote.Generation == existing.Generation && remote.LastSeen > existing.LastSeen))
                {
                    var oldState = existing.State;
                    existing.QueueEndpoint = endpoint;
                    existing.GossipEndpoint = gossipEndpoint;
                    existing.State = remote.State;
                    existing.LastSeen = remote.LastSeen;
                    existing.Generation = remote.Generation;

                    if (oldState != remote.State)
                    {
                        OnMembershipChanged(remote.State switch
                        {
                            NodeState.Active when oldState is NodeState.Suspected or NodeState.Dead =>
                                ClusterMembershipChangeType.NodeRecovered,
                            NodeState.Suspected => ClusterMembershipChangeType.NodeSuspected,
                            NodeState.Dead => ClusterMembershipChangeType.NodeDead,
                            NodeState.Draining => ClusterMembershipChangeType.NodeLeft,
                            _ => ClusterMembershipChangeType.NodeJoined
                        }, existing);
                    }
                }
            }
            else
            {
                // New node discovered
                var newNode = new NodeInfo
                {
                    NodeId = remote.NodeId,
                    QueueEndpoint = endpoint,
                    GossipEndpoint = gossipEndpoint,
                    State = remote.State,
                    LastSeen = remote.LastSeen,
                    Generation = remote.Generation
                };
                if (_members.TryAdd(remote.NodeId, newNode))
                {
                    _logger.LogInformation("Discovered new node {NodeId} at {Endpoint}", remote.NodeId, endpoint);
                    OnMembershipChanged(ClusterMembershipChangeType.NodeJoined, newNode);
                }
            }
        }
    }

    private async Task RunHeartbeatLoopAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(_config.HeartbeatInterval, cancellationToken).ConfigureAwait(false);

                var peers = _members.Values
                    .Where(n => n.NodeId != LocalNode.NodeId &&
                                n.State is NodeState.Active or NodeState.Suspected)
                    .ToList();

                // Send gossip to a random subset (up to 3 peers per cycle for scalability)
                var random = Random.Shared;
                var targets = peers.Count <= 3 ? peers : peers.OrderBy(_ => random.Next()).Take(3).ToList();

                foreach (var peer in targets)
                {
                    try
                    {
                        await SendGossipAsync(peer.GossipEndpoint, cancellationToken).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogDebug(ex, "Heartbeat to {NodeId} failed", peer.NodeId);
                    }
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
    }

    private async Task RunFailureDetectionAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(_config.HeartbeatInterval, cancellationToken).ConfigureAwait(false);

                var now = DateTime.UtcNow;
                var suspicionTimeout = _config.HeartbeatInterval * _config.SuspicionThreshold;
                var deathTimeout = _config.HeartbeatInterval * (_config.SuspicionThreshold + _config.DeathThreshold);

                foreach (var member in _members.Values)
                {
                    if (member.NodeId == LocalNode.NodeId)
                        continue;

                    var elapsed = now - member.LastSeen;

                    if (member.State == NodeState.Active && elapsed > suspicionTimeout)
                    {
                        member.State = NodeState.Suspected;
                        _logger.LogWarning("Node {NodeId} suspected (no heartbeat for {Elapsed})",
                            member.NodeId, elapsed);
                        OnMembershipChanged(ClusterMembershipChangeType.NodeSuspected, member);
                    }
                    else if (member.State == NodeState.Suspected && elapsed > deathTimeout)
                    {
                        member.State = NodeState.Dead;
                        _logger.LogWarning("Node {NodeId} declared dead (no heartbeat for {Elapsed})",
                            member.NodeId, elapsed);
                        OnMembershipChanged(ClusterMembershipChangeType.NodeDead, member);
                    }
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
    }

    private void OnMembershipChanged(ClusterMembershipChangeType changeType, NodeInfo node)
    {
        MembershipChanged?.Invoke(this, new ClusterMembershipChangedEventArgs
        {
            ChangeType = changeType,
            Node = node
        });
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        _cts?.Cancel();
        _listener?.Stop();

        try { _heartbeatTask?.Wait(TimeSpan.FromSeconds(2)); } catch { /* ignore */ }
        try { _listenerTask?.Wait(TimeSpan.FromSeconds(2)); } catch { /* ignore */ }
        try { _failureDetectionTask?.Wait(TimeSpan.FromSeconds(2)); } catch { /* ignore */ }

        _cts?.Dispose();
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        if (_cts != null)
        {
            await _cts.CancelAsync().ConfigureAwait(false);
        }

        _listener?.Stop();

        if (_heartbeatTask != null)
            await _heartbeatTask.WaitAsync(TimeSpan.FromSeconds(2)).ConfigureAwait(false);
        if (_listenerTask != null)
            await _listenerTask.WaitAsync(TimeSpan.FromSeconds(2)).ConfigureAwait(false);
        if (_failureDetectionTask != null)
            await _failureDetectionTask.WaitAsync(TimeSpan.FromSeconds(2)).ConfigureAwait(false);

        _cts?.Dispose();
    }

    // ---- Gossip wire format ----

    private sealed class GossipMessage
    {
        public Guid SenderId { get; set; }
        public List<GossipNodeEntry> Members { get; set; } = new();
    }

    private sealed class GossipNodeEntry
    {
        public Guid NodeId { get; set; }
        public string QueueEndpointAddress { get; set; } = string.Empty;
        public int QueueEndpointPort { get; set; }
        public string GossipEndpointAddress { get; set; } = string.Empty;
        public int GossipEndpointPort { get; set; }
        public NodeState State { get; set; }
        public DateTime LastSeen { get; set; }
        public long Generation { get; set; }
    }
}
