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
    /// Requests one page of catch-up synchronization from a source node for a specific partition.
    /// Used when a node takes over a partition and needs the messages it is missing.
    /// </summary>
    /// <param name="sourceEndpoint">The endpoint of the node to sync from.</param>
    /// <param name="queueName">The partition queue name.</param>
    /// <param name="afterMessageId">
    /// The cursor: only messages whose ID sorts after this ID are returned. Pass <c>null</c> for
    /// the first page and the ID of the last message of the previous page for the next one.
    /// </param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>
    /// The next page of messages, ordered by message ID. An empty page means the partition has
    /// no more messages after <paramref name="afterMessageId"/>.
    /// </returns>
    /// <exception cref="ClusterReplicationException">Thrown when the page could not be read from the source node.</exception>
    Task<IReadOnlyList<Message>> SyncPartitionAsync(
        System.Net.IPEndPoint sourceEndpoint,
        string queueName,
        MessageId? afterMessageId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Requests one page of the message IDs a replica holds for a partition, used by the primary
    /// to find replica copies of messages it has already consumed.
    /// </summary>
    /// <param name="replicaEndpoint">The endpoint of the replica node.</param>
    /// <param name="queueName">The partition queue name.</param>
    /// <param name="sentBefore">Only messages sent before this UTC time are listed.</param>
    /// <param name="afterMessageId">
    /// The cursor: only messages whose ID sorts after this ID are returned. Pass <c>null</c> for
    /// the first page and the last ID of the previous page for the next one.
    /// </param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>
    /// The next page of message IDs, ordered by ID. An empty page means there are no more
    /// matching messages after <paramref name="afterMessageId"/>.
    /// </returns>
    /// <exception cref="ClusterReplicationException">Thrown when the page could not be read from the replica.</exception>
    Task<IReadOnlyList<MessageId>> ListReplicaMessageIdsAsync(
        System.Net.IPEndPoint replicaEndpoint,
        string queueName,
        DateTime sentBefore,
        MessageId? afterMessageId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes replica copies of messages the primary no longer holds.
    /// </summary>
    /// <param name="replicaEndpoint">The endpoint of the replica node.</param>
    /// <param name="queueName">The partition queue name.</param>
    /// <param name="messageIds">The IDs of the messages to remove. IDs the replica does not hold are ignored.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>
    /// A task that resolves to true if the replica acknowledged the removal, or false if it failed.
    /// </returns>
    Task<bool> TrimReplicaAsync(
        System.Net.IPEndPoint replicaEndpoint,
        string queueName,
        IReadOnlyList<MessageId> messageIds,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Starts listening for incoming replication requests from other nodes.
    /// </summary>
    /// <param name="cancellationToken">A token to cancel the listener.</param>
    /// <returns>A task representing the listening operation.</returns>
    Task StartListeningAsync(CancellationToken cancellationToken = default);
}
