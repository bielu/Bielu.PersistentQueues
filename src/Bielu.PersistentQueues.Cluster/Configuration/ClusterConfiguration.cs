using System;
using System.Collections.Generic;
using System.Net;

namespace Bielu.PersistentQueues.Cluster;

/// <summary>
/// Configuration options for cluster membership and replication.
/// </summary>
/// <remarks>
/// The gossip and replication listeners do not authenticate peers. Both ports must only be
/// reachable from other cluster nodes (a trusted network segment or firewall rules).
/// </remarks>
public sealed class ClusterConfiguration
{
    /// <summary>
    /// The default maximum size, in bytes, of a single gossip or replication payload.
    /// </summary>
    public const int DefaultMaxPayloadBytes = 64 * 1024 * 1024;

    /// <summary>
    /// Gets or sets the unique identifier for this node.
    /// If not set, a new GUID is generated automatically.
    /// </summary>
    public Guid NodeId { get; set; } = Guid.NewGuid();

    /// <summary>
    /// Gets or sets the replication factor — the total number of copies of each partition
    /// maintained across the cluster (including the primary).
    /// </summary>
    /// <remarks>
    /// Supported values are <c>1</c> (primary only, no replication) and <c>2</c>
    /// (one primary and one replica). Other values are rejected by <see cref="Validate"/>.
    /// </remarks>
    public int ReplicationFactor { get; set; } = 2;

    /// <summary>
    /// Gets or sets the list of seed node gossip endpoints used for cluster discovery.
    /// </summary>
    /// <remarks>
    /// Seed nodes are well-known gossip endpoints (queue port + <see cref="GossipPortOffset"/>)
    /// that nodes contact to join the cluster. They are contacted again whenever a node has no
    /// reachable peers, so a cluster can heal after a network partition.
    /// </remarks>
    public List<IPEndPoint> SeedNodes { get; set; } = new();

    /// <summary>
    /// Gets or sets the replication mode.
    /// </summary>
    public ReplicationMode ReplicationMode { get; set; } = ReplicationMode.Synchronous;

    /// <summary>
    /// Gets or sets the interval between heartbeat messages sent to other nodes.
    /// </summary>
    public TimeSpan HeartbeatInterval { get; set; } = TimeSpan.FromSeconds(2);

    /// <summary>
    /// Gets or sets the number of missed heartbeats before a node is suspected.
    /// </summary>
    public int SuspicionThreshold { get; set; } = 3;

    /// <summary>
    /// Gets or sets the number of additional missed heartbeats (beyond suspicion)
    /// before a node is declared dead.
    /// </summary>
    public int DeathThreshold { get; set; } = 2;

    /// <summary>
    /// Gets or sets the port offset from the queue endpoint for gossip communication.
    /// </summary>
    /// <remarks>
    /// By default, the gossip endpoint uses the queue port + this offset, and the replication
    /// endpoint uses the queue port + this offset + 1.
    /// </remarks>
    public int GossipPortOffset { get; set; } = 1;

    /// <summary>
    /// Gets or sets the maximum size, in bytes, of a single gossip or replication payload
    /// accepted from a peer. Larger payloads are rejected.
    /// </summary>
    public int MaxPayloadBytes { get; set; } = DefaultMaxPayloadBytes;

    /// <summary>
    /// Validates the configuration.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when a setting is outside its supported range.</exception>
    public void Validate()
    {
        ValidateReplicationFactor(ReplicationFactor);
        if (HeartbeatInterval <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(HeartbeatInterval), HeartbeatInterval, "Heartbeat interval must be positive.");
        if (SuspicionThreshold <= 0)
            throw new ArgumentOutOfRangeException(nameof(SuspicionThreshold), SuspicionThreshold, "Suspicion threshold must be positive.");
        if (DeathThreshold <= 0)
            throw new ArgumentOutOfRangeException(nameof(DeathThreshold), DeathThreshold, "Death threshold must be positive.");
        if (MaxPayloadBytes <= 0)
            throw new ArgumentOutOfRangeException(nameof(MaxPayloadBytes), MaxPayloadBytes, "Maximum payload size must be positive.");
    }

    internal static void ValidateReplicationFactor(int replicationFactor)
    {
        if (replicationFactor is < 1 or > 2)
            throw new ArgumentOutOfRangeException(nameof(ReplicationFactor), replicationFactor,
                "Only replication factors of 1 (no replica) and 2 (one replica) are supported.");
    }
}

/// <summary>
/// Defines the replication mode for message writes.
/// </summary>
public enum ReplicationMode
{
    /// <summary>
    /// The write blocks until the replica acknowledges it. If the replica is assigned but
    /// unreachable or rejects the write, a <see cref="ClusterReplicationException"/> is thrown
    /// after the message has been stored on the primary, so retrying can create a duplicate.
    /// </summary>
    Synchronous,

    /// <summary>
    /// Messages are acknowledged immediately after writing to the primary.
    /// Replication happens asynchronously. Lower latency but risks message loss
    /// if the primary fails before replication completes.
    /// </summary>
    Asynchronous
}
