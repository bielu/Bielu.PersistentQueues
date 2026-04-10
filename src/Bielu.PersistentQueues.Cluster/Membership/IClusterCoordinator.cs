using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Bielu.PersistentQueues.Cluster;

/// <summary>
/// Coordinates cluster membership, tracking which nodes are alive and managing
/// the cluster lifecycle.
/// </summary>
/// <remarks>
/// Implementations may use gossip protocols, external coordination services (etcd, Consul),
/// or other mechanisms to maintain a consistent view of cluster membership.
/// </remarks>
public interface IClusterCoordinator : IAsyncDisposable, IDisposable
{
    /// <summary>
    /// Gets information about the local node.
    /// </summary>
    NodeInfo LocalNode { get; }

    /// <summary>
    /// Gets a snapshot of all known nodes in the cluster.
    /// </summary>
    /// <returns>A read-only collection of all known nodes, including the local node.</returns>
    IReadOnlyList<NodeInfo> GetMembers();

    /// <summary>
    /// Gets only the active (non-dead, non-suspected) nodes in the cluster.
    /// </summary>
    /// <returns>A read-only collection of active nodes.</returns>
    IReadOnlyList<NodeInfo> GetActiveMembers();

    /// <summary>
    /// Joins the cluster, announcing this node's presence to seed nodes.
    /// </summary>
    /// <param name="cancellationToken">A token to cancel the join operation.</param>
    /// <returns>A task that completes when the node has successfully joined the cluster.</returns>
    Task JoinAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Gracefully leaves the cluster, notifying other nodes of departure.
    /// </summary>
    /// <param name="cancellationToken">A token to cancel the leave operation.</param>
    /// <returns>A task that completes when the node has notified peers of its departure.</returns>
    Task LeaveAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Raised when the cluster membership changes (node joined, left, suspected, or confirmed dead).
    /// </summary>
    event EventHandler<ClusterMembershipChangedEventArgs> MembershipChanged;
}
