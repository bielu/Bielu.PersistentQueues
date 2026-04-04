namespace Bielu.PersistentQueues.Cluster;

/// <summary>
/// Represents the state of a node in the cluster.
/// </summary>
public enum NodeState
{
    /// <summary>
    /// The node is active and serving requests.
    /// </summary>
    Active,

    /// <summary>
    /// The node is draining — it is no longer accepting new partition assignments
    /// but is still serving existing ones until handoff completes.
    /// </summary>
    Draining,

    /// <summary>
    /// The node is suspected to be down but has not yet been confirmed dead.
    /// </summary>
    Suspected,

    /// <summary>
    /// The node has been confirmed dead and its partitions should be reassigned.
    /// </summary>
    Dead
}
