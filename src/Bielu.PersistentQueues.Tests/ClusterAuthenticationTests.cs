using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Bielu.PersistentQueues.Cluster;
using Bielu.PersistentQueues.Logging;
using Bielu.PersistentQueues.Partitioning;
using Bielu.PersistentQueues.Serialization;
using Bielu.PersistentQueues.Storage.LMDB;
using Microsoft.Extensions.Logging;
using Shouldly;
using Xunit;
using Xunit.Abstractions;

namespace Bielu.PersistentQueues.Tests;

public class ClusterAuthenticationTests(ITestOutputHelper output) : TestBase(output)
{
    private const string QueueName = "orders";
    private const string Secret = "cluster-secret-for-tests";

    [Fact]
    public async Task authenticated_replication_accepts_peers_with_the_same_secret()
    {
        await AuthenticatedReplicationScenarioAsync(async ctx =>
        {
            var client = ctx.Client(Secret, ctx.PrimaryNodeId);
            var message = NewMessage(ctx.PartitionQueueName);

            (await client.ReplicateAsync(ctx.ReplicaEndpoint, ctx.PartitionQueueName, [message], epoch: 1, ctx.Token)).ShouldBeTrue();

            ctx.Store.PersistedIncoming(ctx.PartitionQueueName).ShouldHaveSingleItem().Id.ShouldBe(message.Id);
            (await client.SyncPartitionAsync(ctx.ReplicaEndpoint, ctx.PartitionQueueName, null, ctx.Token))
                .ShouldHaveSingleItem().Id.ShouldBe(message.Id);
        });
    }

    [Theory]
    [InlineData(null)]
    [InlineData("a-different-cluster-secret")]
    public async Task authenticated_replication_rejects_unsigned_or_wrongly_signed_requests(string? clientSecret)
    {
        await AuthenticatedReplicationScenarioAsync(async ctx =>
        {
            var client = ctx.Client(clientSecret, ctx.PrimaryNodeId);
            var message = NewMessage(ctx.PartitionQueueName);

            (await client.ReplicateAsync(ctx.ReplicaEndpoint, ctx.PartitionQueueName, [message], epoch: 1, ctx.Token)).ShouldBeFalse();
            await Should.ThrowAsync<ClusterReplicationException>(() =>
                client.SyncPartitionAsync(ctx.ReplicaEndpoint, ctx.PartitionQueueName, null, ctx.Token));
            await Should.ThrowAsync<ClusterReplicationException>(() =>
                client.ListReplicaMessageIdsAsync(ctx.ReplicaEndpoint, ctx.PartitionQueueName, DateTime.MaxValue, null, ctx.Token));

            ctx.Store.PersistedIncoming(ctx.PartitionQueueName).ShouldBeEmpty();
        });
    }

    [Fact]
    public async Task authenticated_replication_only_accepts_trims_from_the_partition_primary()
    {
        await AuthenticatedReplicationScenarioAsync(async ctx =>
        {
            var message = NewMessage(ctx.PartitionQueueName);
            ctx.Store.StoreIncoming(message);

            var stranger = ctx.Client(Secret, Guid.NewGuid());
            (await stranger.TrimReplicaAsync(ctx.ReplicaEndpoint, ctx.PartitionQueueName, [message.Id], ctx.Token)).ShouldBeFalse();
            ctx.Store.PersistedIncoming(ctx.PartitionQueueName).ShouldHaveSingleItem();

            // The replica itself is not the primary of the partition either
            var self = ctx.Client(Secret, ctx.ReplicaNodeId);
            (await self.TrimReplicaAsync(ctx.ReplicaEndpoint, ctx.PartitionQueueName, [message.Id], ctx.Token)).ShouldBeFalse();

            var primary = ctx.Client(Secret, ctx.PrimaryNodeId);
            (await primary.TrimReplicaAsync(ctx.ReplicaEndpoint, ctx.PartitionQueueName, [message.Id], ctx.Token)).ShouldBeTrue();
            ctx.Store.PersistedIncoming(ctx.PartitionQueueName).ShouldBeEmpty();
        });
    }

    [Fact]
    public async Task authenticated_replication_rejects_signed_frames_outside_the_clock_skew()
    {
        await AuthenticatedReplicationScenarioAsync(async ctx =>
        {
            var request = Encoding.UTF8.GetBytes(
                $$"""{"Type":3,"QueueName":"{{ctx.PartitionQueueName}}","SentBefore":"9999-12-31T00:00:00Z"}""");

            // A fresh frame signed the documented way gets a response
            var fresh = await SendRawFrameAsync(ctx, Sign(Secret, request, DateTimeOffset.UtcNow));
            fresh.ShouldBeGreaterThan(0);

            // A replayed frame from ten minutes ago is dropped without a response
            var stale = await SendRawFrameAsync(ctx, Sign(Secret, request, DateTimeOffset.UtcNow.AddMinutes(-10)));
            stale.ShouldBe(0);
        });
    }

    [Fact]
    public async Task gossip_only_merges_members_that_share_the_secret()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var seedPort = GetFreePort();
        var seedEndpoint = new IPEndPoint(IPAddress.Loopback, seedPort);

        using var seed = NewCoordinator(seedPort, Secret);
        using var member = NewCoordinator(GetFreePort(), Secret, seedEndpoint);
        using var outsider = NewCoordinator(GetFreePort(), "a-different-cluster-secret", seedEndpoint);

        await seed.JoinAsync(cts.Token);
        await member.JoinAsync(cts.Token);
        await outsider.JoinAsync(cts.Token);

        SpinWait.SpinUntil(() => seed.GetMembers().Any(m => m.NodeId == member.LocalNode.NodeId) &&
                                 member.GetMembers().Any(m => m.NodeId == seed.LocalNode.NodeId),
            TimeSpan.FromSeconds(5)).ShouldBeTrue();

        // Give the outsider a few heartbeats to try
        await Task.Delay(500, cts.Token);
        seed.GetMembers().ShouldNotContain(m => m.NodeId == outsider.LocalNode.NodeId);
        member.GetMembers().ShouldNotContain(m => m.NodeId == outsider.LocalNode.NodeId);
        outsider.GetMembers().ShouldHaveSingleItem();

        await cts.CancelAsync();
    }

    [Fact]
    public void validate_rejects_short_shared_secret()
    {
        Should.Throw<ArgumentException>(new ClusterConfiguration { SharedSecret = "short" }.Validate)
            .ParamName.ShouldBe(nameof(ClusterConfiguration.SharedSecret));
        Should.NotThrow(new ClusterConfiguration { SharedSecret = Secret }.Validate);
    }

    private GossipClusterCoordinator NewCoordinator(int port, string secret, params IPEndPoint[] seeds)
    {
        var config = new ClusterConfiguration
        {
            SharedSecret = secret,
            GossipPortOffset = 0,
            HeartbeatInterval = TimeSpan.FromMilliseconds(100),
            SeedNodes = [.. seeds]
        };
        return new GossipClusterCoordinator(config, new IPEndPoint(IPAddress.Loopback, port),
            new RecordingLogger<GossipClusterCoordinator>(OutputWriter, LogLevel.Information));
    }

    private static byte[] Sign(string secret, byte[] payload, DateTimeOffset sentAt)
    {
        const byte replicationRequestChannel = 2;
        var frame = new byte[payload.Length + sizeof(long) + HMACSHA256.HashSizeInBytes];
        payload.CopyTo(frame, 0);
        BinaryPrimitives.WriteInt64LittleEndian(frame.AsSpan(payload.Length), sentAt.ToUnixTimeMilliseconds());
        var mac = HMACSHA256.HashData(Encoding.UTF8.GetBytes(secret),
            (byte[])[replicationRequestChannel, .. frame.AsSpan(0, payload.Length + sizeof(long))]);
        mac.CopyTo(frame, payload.Length + sizeof(long));
        return frame;
    }

    private static async Task<int> SendRawFrameAsync(AuthenticatedReplication ctx, byte[] frame)
    {
        using var client = new TcpClient();
        await client.ConnectAsync(ctx.ReplicaEndpoint, ctx.Token);
        var stream = client.GetStream();
        await stream.WriteAsync(BitConverter.GetBytes(frame.Length), ctx.Token);
        await stream.WriteAsync(frame, ctx.Token);

        var buffer = new byte[4];
        return await stream.ReadAsync(buffer, ctx.Token);
    }

    private async Task AuthenticatedReplicationScenarioAsync(Func<AuthenticatedReplication, Task> scenario)
    {
        using var env = LightningEnvironment();
        using var store = new LmdbMessageStore(env, new MessageSerializer());
        store.CreateQueue(QueueName);
        store.CreateQueue(PartitionConstants.FormatPartitionQueueName(QueueName, 0));
        store.CreateQueue(PartitionConstants.FormatPartitionQueueName(QueueName, 1));

        var primaryNode = NewNode();
        var replicaNode = NewNode();
        var assignment = new PartitionAssignmentTable(
            new RecordingLogger<PartitionAssignmentTable>(OutputWriter, LogLevel.Information), replicationFactor: 2);
        assignment.AssignPartitions(QueueName, 2, [primaryNode, replicaNode]);
        var partition = assignment.GetAssignments(QueueName).First(a => a.PrimaryNodeId == primaryNode.NodeId);

        var replicaEndpoint = new IPEndPoint(IPAddress.Loopback, GetFreePort());
        await using var replica = new TcpReplicationProtocol(store, replicaEndpoint,
            new ClusterConfiguration { NodeId = replicaNode.NodeId, SharedSecret = Secret }, assignment,
            new RecordingLogger<TcpReplicationProtocol>(OutputWriter, LogLevel.Information));

        var clients = new List<TcpReplicationProtocol>();
        TcpReplicationProtocol Client(string? secret, Guid nodeId)
        {
            var client = new TcpReplicationProtocol(store, new IPEndPoint(IPAddress.Loopback, 0),
                new ClusterConfiguration { NodeId = nodeId, SharedSecret = secret }, assignment,
                new RecordingLogger<TcpReplicationProtocol>(OutputWriter, LogLevel.Information));
            clients.Add(client);
            return client;
        }

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var listening = replica.StartListeningAsync(cts.Token);
        try
        {
            await scenario(new AuthenticatedReplication(store, replicaEndpoint, partition.PartitionKey,
                primaryNode.NodeId, replicaNode.NodeId, Client, cts.Token));
        }
        finally
        {
            clients.ForEach(c => c.Dispose());
            await cts.CancelAsync();
            await listening;
        }
    }

    private static NodeInfo NewNode()
    {
        var port = GetFreePort();
        return new NodeInfo
        {
            NodeId = Guid.NewGuid(),
            QueueEndpoint = new IPEndPoint(IPAddress.Loopback, port),
            GossipEndpoint = new IPEndPoint(IPAddress.Loopback, port + 1),
            State = NodeState.Active
        };
    }

    private static int GetFreePort()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        return ((IPEndPoint)listener.LocalEndpoint).Port;
    }

    private sealed record AuthenticatedReplication(
        LmdbMessageStore Store,
        IPEndPoint ReplicaEndpoint,
        string PartitionQueueName,
        Guid PrimaryNodeId,
        Guid ReplicaNodeId,
        Func<string?, Guid, TcpReplicationProtocol> Client,
        CancellationToken Token);
}
