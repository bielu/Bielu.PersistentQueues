using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Bielu.PersistentQueues.Cluster;

/// <summary>
/// Defines the protocol for replicating messages between primary and replica nodes.
/// </summary>
/// <remarks>
/// The replication protocol handles the data plane of the cluster — ensuring that
/// messages written to a primary partition are replicated to the corresponding replica node.
/// It supports both synchronous (wait for ack) and asynchronous (fire-and-forget) replication.
/// </remarks>
public interface IReplicationProtocol : IAsyncDisposable, IDisposable
{
    /// <summary>
    /// Replicates a batch of messages from the primary to the replica node.
    /// </summary>
    /// <param name="replicaEndpoint">The endpoint of the replica node.</param>
    /// <param name="queueName">The partition queue name (e.g., "orders:partition-0").</param>
    /// <param name="messages">The messages to replicate.</param>
    /// <param name="epoch">The current epoch of the partition assignment.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>
    /// A task that resolves to true if the replica acknowledged the messages,
    /// or false if replication failed.
    /// </returns>
    Task<bool> ReplicateAsync(
        System.Net.IPEndPoint replicaEndpoint,
        string queueName,
        IReadOnlyList<Message> messages,
        long epoch,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Requests catch-up synchronization from a source node for a specific partition.
    /// Used when a new replica needs to sync all messages it missed.
    /// </summary>
    /// <param name="sourceEndpoint">The endpoint of the node to sync from.</param>
    /// <param name="queueName">The partition queue name.</param>
    /// <param name="afterMessageId">
    /// The last known message ID on the requesting node. Only messages after this ID will be synced.
    /// If null, all messages for the partition are requested.
    /// </param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>The messages that the replica is missing.</returns>
    Task<IReadOnlyList<Message>> SyncPartitionAsync(
        System.Net.IPEndPoint sourceEndpoint,
        string queueName,
        MessageId? afterMessageId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Starts listening for incoming replication requests from other nodes.
    /// </summary>
    /// <param name="cancellationToken">A token to cancel the listener.</param>
    /// <returns>A task representing the listening operation.</returns>
    Task StartListeningAsync(CancellationToken cancellationToken = default);
}
