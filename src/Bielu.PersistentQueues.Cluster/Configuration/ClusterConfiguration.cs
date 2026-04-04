using System;
using System.Collections.Generic;
using System.Net;

namespace Bielu.PersistentQueues.Cluster;

/// <summary>
/// Configuration options for cluster membership and replication.
/// </summary>
public sealed class ClusterConfiguration
{
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
    /// A replication factor of 2 means each partition has one primary and one replica.
    /// </remarks>
    public int ReplicationFactor { get; set; } = 2;

    /// <summary>
    /// Gets or sets the list of seed node endpoints used for initial cluster discovery.
    /// </summary>
    /// <remarks>
    /// Seed nodes are well-known endpoints that new nodes contact to join the cluster.
    /// At least one seed node must be reachable for a new node to discover the cluster.
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
    /// By default, the gossip endpoint uses the queue port + this offset.
    /// </remarks>
    public int GossipPortOffset { get; set; } = 1;
}

/// <summary>
/// Defines the replication mode for message writes.
/// </summary>
public enum ReplicationMode
{
    /// <summary>
    /// Messages are replicated to the replica before the write is acknowledged.
    /// Provides stronger durability guarantees at the cost of higher write latency.
    /// </summary>
    Synchronous,

    /// <summary>
    /// Messages are acknowledged immediately after writing to the primary.
    /// Replication happens asynchronously. Lower latency but risks message loss
    /// if the primary fails before replication completes.
    /// </summary>
    Asynchronous
}
