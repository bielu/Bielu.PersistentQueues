using System;

namespace Bielu.PersistentQueues.Cluster;

/// <summary>
/// Provides data for cluster membership change events.
/// </summary>
public sealed class ClusterMembershipChangedEventArgs : EventArgs
{
    /// <summary>
    /// Gets the type of membership change.
    /// </summary>
    public required ClusterMembershipChangeType ChangeType { get; init; }

    /// <summary>
    /// Gets the node that was affected by the change.
    /// </summary>
    public required NodeInfo Node { get; init; }
}

/// <summary>
/// The type of cluster membership change.
/// </summary>
public enum ClusterMembershipChangeType
{
    /// <summary>A new node joined the cluster.</summary>
    NodeJoined,

    /// <summary>A node left the cluster gracefully.</summary>
    NodeLeft,

    /// <summary>A node is suspected to be down.</summary>
    NodeSuspected,

    /// <summary>A node has been confirmed dead.</summary>
    NodeDead,

    /// <summary>A previously suspected or dead node has recovered.</summary>
    NodeRecovered
}
