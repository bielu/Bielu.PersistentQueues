using System;
using System.Collections.Generic;
using System.Net;

namespace Bielu.PersistentQueues.Cluster;

/// <summary>
/// Represents the identity and status of a node in the cluster.
/// </summary>
/// <remarks>
/// Each cluster node is uniquely identified by its <see cref="NodeId"/>. The node's
/// <see cref="Endpoint"/> is used for both queue traffic and cluster gossip communication.
/// <see cref="State"/> tracks the node's lifecycle, and <see cref="LastSeen"/> records
/// the last time a heartbeat was received from this node.
/// </remarks>
public sealed class NodeInfo
{
    /// <summary>
    /// Gets the unique identifier for this node.
    /// </summary>
    public Guid NodeId { get; init; }

    /// <summary>
    /// Gets or sets the network endpoint where this node listens for queue and cluster traffic.
    /// </summary>
    public required IPEndPoint QueueEndpoint { get; set; }

    /// <summary>
    /// Gets or sets the network endpoint used for gossip protocol communication.
    /// </summary>
    /// <remarks>
    /// Defaults to the same as <see cref="QueueEndpoint"/> with port offset +1.
    /// </remarks>
    public required IPEndPoint GossipEndpoint { get; set; }

    /// <summary>
    /// Gets or sets the current state of this node.
    /// </summary>
    public NodeState State { get; set; } = NodeState.Active;

    /// <summary>
    /// Gets or sets the last time a heartbeat was received from this node.
    /// </summary>
    public DateTime LastSeen { get; set; } = DateTime.UtcNow;

    /// <summary>
    /// Gets or sets the monotonically increasing generation number for this node.
    /// Incremented each time the node restarts or rejoins the cluster.
    /// </summary>
    public long Generation { get; set; }

    /// <summary>
    /// Gets or sets metadata tags associated with this node (e.g., rack, zone).
    /// </summary>
    public Dictionary<string, string> Metadata { get; init; } = new();

    public override bool Equals(object? obj) =>
        obj is NodeInfo other && NodeId == other.NodeId;

    public override int GetHashCode() => NodeId.GetHashCode();

    public override string ToString() =>
        $"Node({NodeId:N}, {QueueEndpoint}, {State})";
}
