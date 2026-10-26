using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
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

public class ClusteredPartitionedQueueDecoratorTests(ITestOutputHelper output) : TestBase(output)
{
    private const string QueueName = "orders";
    private const int PartitionCount = 4;

    [Fact]
    public async Task enqueue_on_primary_stores_locally_and_replicates_partition_message()
    {
        await ClusterScenarioAsync(ctx =>
        {
            var partition = ctx.PartitionsWhere(a => a.PrimaryNodeId == ctx.Local.NodeId).First();
            var partitionQueueName = PartitionConstants.FormatPartitionQueueName(QueueName, partition);
            var message = NewMessage(QueueName, "payload");

            ctx.Clustered.EnqueueToPartition(message, partition);

            ctx.Queue.Store.PersistedIncoming(partitionQueueName)
                .Select(m => m.Id).ShouldContain(message.Id);

            var call = ctx.Replication.Calls.ShouldHaveSingleItem();
            call.QueueName.ShouldBe(partitionQueueName);
            call.Endpoint.Port.ShouldBe(ctx.Remote.QueueEndpoint.Port + ctx.Config.GossipPortOffset + 1);
            var replicated = call.Messages.ShouldHaveSingleItem();
            replicated.Id.ShouldBe(message.Id);
            replicated.QueueString.ShouldBe(partitionQueueName);
        });
    }

    [Fact]
    public async Task enqueue_on_non_primary_forwards_to_primary_without_storing_locally()
    {
        await ClusterScenarioAsync(ctx =>
        {
            var partition = ctx.PartitionsWhere(a => a.PrimaryNodeId == ctx.Remote.NodeId).First();
            var partitionQueueName = PartitionConstants.FormatPartitionQueueName(QueueName, partition);
            var message = NewMessage(QueueName, "payload");

            ctx.Clustered.EnqueueToPartition(message, partition);

            ctx.Queue.Store.PersistedIncoming(partitionQueueName).ShouldBeEmpty();
            ctx.Replication.Calls.ShouldBeEmpty();

            var forwarded = ctx.Queue.Store.PersistedOutgoing().ShouldHaveSingleItem();
            forwarded.Id.ShouldBe(message.Id);
            forwarded.QueueString.ShouldBe(partitionQueueName);
            forwarded.Destination!.Port.ShouldBe(ctx.Remote.QueueEndpoint.Port);
        });
    }

    [Fact]
    public async Task enqueue_routes_through_cluster_using_partition_strategy()
    {
        await ClusterScenarioAsync(ctx =>
        {
            var owned = ctx.PartitionsWhere(a => a.PrimaryNodeId == ctx.Local.NodeId).ToHashSet();
            var messages = Enumerable.Range(0, 20)
                .Select(i => Message.Create(data: Encoding.UTF8.GetBytes("x"), queue: QueueName, partitionKey: $"key-{i}"))
                .ToList();

            foreach (var message in messages)
                ctx.Clustered.Enqueue(message);

            var expectedLocal = messages.Count(m => owned.Contains(ctx.Clustered.ResolvePartition(m)));
            ctx.Replication.Calls.Count.ShouldBe(expectedLocal);
            ctx.Queue.Store.PersistedOutgoing().Count().ShouldBe(messages.Count - expectedLocal);
        });
    }

    [Fact]
    public async Task enqueue_without_assignment_falls_back_to_local_enqueue()
    {
        await ClusterScenarioAsync(ctx =>
        {
            ctx.Partitioned.CreatePartitionedQueue("unassigned", 2);
            var message = NewMessage("unassigned", "payload");

            ctx.Clustered.EnqueueToPartition(message, 1);

            ctx.Queue.Store.PersistedIncoming(PartitionConstants.FormatPartitionQueueName("unassigned", 1))
                .Select(m => m.Id).ShouldContain(message.Id);
            ctx.Replication.Calls.ShouldBeEmpty();
        });
    }

    [Fact]
    public async Task active_partitions_only_include_partitions_owned_as_primary()
    {
        await ClusterScenarioAsync(ctx =>
        {
            // Put a message in every partition directly, bypassing cluster routing
            for (var p = 0; p < PartitionCount; p++)
                ctx.Partitioned.EnqueueToPartition(NewMessage(QueueName), p);

            var owned = ctx.PartitionsWhere(a => a.PrimaryNodeId == ctx.Local.NodeId);

            ctx.Partitioned.GetActivePartitions(QueueName).Length.ShouldBe(PartitionCount);
            ctx.Clustered.GetActivePartitions(QueueName).ShouldBe(owned, ignoreOrder: true);
            ctx.Clustered.GetAvailablePartitions(QueueName).ShouldBe(owned, ignoreOrder: true);
        });
    }

    [Fact]
    public async Task replication_protocol_round_trips_messages_with_headers()
    {
        using var replicaEnv = LightningEnvironment();
        using var replicaStore = new LmdbMessageStore(replicaEnv, new MessageSerializer());
        var partitionQueueName = PartitionConstants.FormatPartitionQueueName(QueueName, 0);
        replicaStore.CreateQueue(partitionQueueName);

        var replicaEndpoint = new IPEndPoint(IPAddress.Loopback, GetFreePort());
        await using var replica = new TcpReplicationProtocol(replicaStore, replicaEndpoint,
            new RecordingLogger<TcpReplicationProtocol>(OutputWriter, LogLevel.Information));
        await using var primary = new TcpReplicationProtocol(replicaStore, new IPEndPoint(IPAddress.Loopback, 0),
            new RecordingLogger<TcpReplicationProtocol>(OutputWriter, LogLevel.Information));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var listening = replica.StartListeningAsync(cts.Token);

        var message = Message.Create(
            data: Encoding.UTF8.GetBytes("payload"),
            queue: partitionQueueName,
            partitionKey: "customer-1",
            headers: new Dictionary<string, string> { ["tenant"] = "acme" });

        var acked = await primary.ReplicateAsync(replicaEndpoint, partitionQueueName, [message], epoch: 1, cts.Token);
        acked.ShouldBeTrue();

        var stored = replicaStore.PersistedIncoming(partitionQueueName).ShouldHaveSingleItem();
        stored.Id.ShouldBe(message.Id);
        stored.GetHeadersDictionary()["tenant"].ShouldBe("acme");

        var synced = await primary.SyncPartitionAsync(replicaEndpoint, partitionQueueName, afterMessageId: null, cts.Token);
        var syncedMessage = synced.ShouldHaveSingleItem();
        syncedMessage.Id.ShouldBe(message.Id);
        syncedMessage.SentAt.ShouldBe(message.SentAt);
        syncedMessage.GetHeadersDictionary()["tenant"].ShouldBe("acme");

        var afterLast = await primary.SyncPartitionAsync(replicaEndpoint, partitionQueueName, message.Id, cts.Token);
        afterLast.ShouldBeEmpty();

        await cts.CancelAsync();
        await listening;
    }

    private static int GetFreePort()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        return ((IPEndPoint)listener.LocalEndpoint).Port;
    }

    private async Task ClusterScenarioAsync(Action<ClusterScenario> scenario)
    {
        await QueueScenarioAsync((queue, _) =>
        {
            var partitioned = new PartitionedQueue(queue, new HashPartitionStrategy());
            var local = NewNode(5000);
            var remote = NewNode(5001);
            var coordinator = new FakeClusterCoordinator(local, remote);
            var assignment = new PartitionAssignmentTable(
                new RecordingLogger<PartitionAssignmentTable>(OutputWriter, LogLevel.Information));
            var replication = new RecordingReplicationProtocol();
            var config = new ClusterConfiguration { NodeId = local.NodeId };

            var clustered = new ClusteredPartitionedQueueDecorator(partitioned, coordinator, assignment, replication, config,
                new RecordingLogger<ClusteredPartitionedQueueDecorator>(OutputWriter, LogLevel.Information));
            clustered.CreatePartitionedQueue(QueueName, PartitionCount);

            scenario(new ClusterScenario(queue, partitioned, clustered, assignment, replication, config, local, remote));
            return Task.CompletedTask;
        }, TimeSpan.FromSeconds(5));
    }

    private static NodeInfo NewNode(int port) => new()
    {
        NodeId = Guid.NewGuid(),
        QueueEndpoint = new IPEndPoint(IPAddress.Loopback, port),
        GossipEndpoint = new IPEndPoint(IPAddress.Loopback, port + 1),
        State = NodeState.Active
    };

    private sealed record ClusterScenario(
        IQueue Queue,
        PartitionedQueue Partitioned,
        ClusteredPartitionedQueueDecorator Clustered,
        IPartitionAssignment Assignment,
        RecordingReplicationProtocol Replication,
        ClusterConfiguration Config,
        NodeInfo Local,
        NodeInfo Remote)
    {
        public int[] PartitionsWhere(Func<PartitionReplica, bool> predicate)
        {
            var partitions = Assignment.GetAssignments(QueueName)
                .Where(predicate)
                .Select(a => a.PartitionIndex)
                .ToArray();
            partitions.ShouldNotBeEmpty("expected the assignment to give each node at least one primary partition");
            return partitions;
        }
    }

    private sealed class FakeClusterCoordinator(NodeInfo local, params NodeInfo[] others) : IClusterCoordinator
    {
        private readonly List<NodeInfo> _members = [local, .. others];

        public NodeInfo LocalNode => local;
        public IReadOnlyList<NodeInfo> GetMembers() => _members;
        public IReadOnlyList<NodeInfo> GetActiveMembers() => _members.Where(m => m.State == NodeState.Active).ToList();
        public Task JoinAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
        public Task LeaveAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
        public event EventHandler<ClusterMembershipChangedEventArgs>? MembershipChanged { add { } remove { } }
        public void Dispose() { }
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class RecordingReplicationProtocol : IReplicationProtocol
    {
        public List<(IPEndPoint Endpoint, string QueueName, IReadOnlyList<Message> Messages, long Epoch)> Calls { get; } = [];

        public Task<bool> ReplicateAsync(IPEndPoint replicaEndpoint, string queueName, IReadOnlyList<Message> messages,
            long epoch, CancellationToken cancellationToken = default)
        {
            Calls.Add((replicaEndpoint, queueName, messages, epoch));
            return Task.FromResult(true);
        }

        public Task<IReadOnlyList<Message>> SyncPartitionAsync(IPEndPoint sourceEndpoint, string queueName,
            MessageId? afterMessageId, CancellationToken cancellationToken = default)
            => Task.FromResult<IReadOnlyList<Message>>([]);

        public Task StartListeningAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
        public void Dispose() { }
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
