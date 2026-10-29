using System;
using System.Collections.Generic;
using System.Net;

namespace Bielu.PersistentQueues.Cluster;

/// <summary>
/// Configuration options for cluster membership and replication.
/// </summary>
/// <remarks>
/// Unless <see cref="SharedSecret"/> is set, the gossip and replication listeners do not
/// authenticate peers. Either way, both ports should only be reachable from other cluster nodes
/// (a trusted network segment or firewall rules).
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
    /// Gets or sets the IPv4 address other nodes use to reach this node's queue, gossip and
    /// replication ports.
    /// </summary>
    /// <remarks>
    /// When not set, the queue endpoint address is advertised. Set it when the queue listens on
    /// <see cref="IPAddress.Any"/> or on loopback (the default of <c>AutomaticEndpoint()</c>), which
    /// remote peers cannot connect to. The listeners still bind to the queue endpoint address.
    /// </remarks>
    public IPAddress? AdvertisedAddress { get; set; }

    /// <summary>
    /// Gets or sets how often a primary removes replica copies of messages it has already consumed.
    /// Set to <see cref="TimeSpan.Zero"/> to disable trimming.
    /// </summary>
    public TimeSpan ReplicaTrimInterval { get; set; } = TimeSpan.FromMinutes(1);

    /// <summary>
    /// Gets or sets the minimum age (by <see cref="Message.SentAt"/>) of a replica copy before it can be trimmed.
    /// </summary>
    /// <remarks>
    /// A write forwarded by a non-primary node can reach the replica before the primary. The grace
    /// period keeps such copies until the primary has stored them, so set it above the longest
    /// expected forwarding delay.
    /// </remarks>
    public TimeSpan ReplicaTrimGracePeriod { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// The minimum length of <see cref="SharedSecret"/>.
    /// </summary>
    public const int MinSharedSecretLength = 16;

    /// <summary>
    /// Gets or sets the secret every node of the cluster shares to authenticate gossip and
    /// replication traffic. When not set, peers are not authenticated.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Every gossip message and replication request and response is signed with HMAC-SHA256 over
    /// this secret and a timestamp; frames that are unsigned, forged or more than five minutes
    /// off this node's clock are dropped. All nodes must use the same secret, and their clocks
    /// must be roughly synchronized.
    /// </para>
    /// <para>
    /// Use a random value of at least <see cref="MinSharedSecretLength"/> characters (for example
    /// 32 random bytes, base64-encoded). Traffic is authenticated but not encrypted.
    /// </para>
    /// </remarks>
    public string? SharedSecret { get; set; }

    /// <summary>
    /// Validates the configuration.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when a setting is outside its supported range.</exception>
    /// <exception cref="ArgumentException">Thrown when the shared secret is too short or the advertised address cannot be used.</exception>
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
        if (ReplicaTrimInterval < TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(ReplicaTrimInterval), ReplicaTrimInterval, "Replica trim interval must not be negative.");
        if (ReplicaTrimGracePeriod < TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(ReplicaTrimGracePeriod), ReplicaTrimGracePeriod, "Replica trim grace period must not be negative.");
        if (SharedSecret != null && SharedSecret.Length < MinSharedSecretLength)
            throw new ArgumentException(
                $"The shared secret must be at least {MinSharedSecretLength} characters long.", nameof(SharedSecret));
        if (AdvertisedAddress != null)
            ValidateAdvertisedAddress(AdvertisedAddress);
    }

    /// <summary>
    /// Resolves the address advertised to peers: <see cref="AdvertisedAddress"/> when set,
    /// otherwise <paramref name="queueAddress"/>.
    /// </summary>
    /// <exception cref="ArgumentException">Thrown when the address is not a routable IPv4 address.</exception>
    internal IPAddress ResolveAdvertisedAddress(IPAddress queueAddress)
    {
        var address = AdvertisedAddress ?? queueAddress;
        ValidateAdvertisedAddress(address);
        return address;
    }

    private static void ValidateAdvertisedAddress(IPAddress address)
    {
        // Gossip and replication clients use IPv4 sockets, and a wildcard address tells a peer
        // nothing about where to connect.
        if (address.AddressFamily != System.Net.Sockets.AddressFamily.InterNetwork)
            throw new ArgumentException(
                $"Cluster traffic requires an IPv4 address, but '{address}' was advertised. Set {nameof(AdvertisedAddress)}.",
                nameof(AdvertisedAddress));
        if (address.Equals(IPAddress.Any) || address.Equals(IPAddress.Broadcast))
            throw new ArgumentException(
                $"'{address}' cannot be advertised to peers. Set {nameof(AdvertisedAddress)} to an address other nodes can reach.",
                nameof(AdvertisedAddress));
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
