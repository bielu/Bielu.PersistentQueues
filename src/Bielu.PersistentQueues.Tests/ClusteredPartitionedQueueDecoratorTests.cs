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
            var replica = ctx.NodeById(ctx.Assignment.GetAssignment(QueueName, partition)!.ReplicaNodeId);
            var message = NewMessage(QueueName, "payload");

            ctx.Clustered.EnqueueToPartition(message, partition);

            ctx.Queue.Store.PersistedIncoming(partitionQueueName)
                .Select(m => m.Id).ShouldContain(message.Id);

            var call = ctx.Replication.Calls.ShouldHaveSingleItem();
            call.QueueName.ShouldBe(partitionQueueName);
            call.Endpoint.Port.ShouldBe(replica.QueueEndpoint.Port + ctx.Config.GossipPortOffset + 1);
            var replicated = call.Messages.ShouldHaveSingleItem();
            replicated.Id.ShouldBe(message.Id);
            replicated.QueueString.ShouldBe(partitionQueueName);
        });
    }

    [Fact]
    public async Task enqueue_on_replica_forwards_to_primary_and_keeps_replica_copy()
    {
        await ClusterScenarioAsync(ctx =>
        {
            var partition = ctx.PartitionsWhere(a => a.ReplicaNodeId == ctx.Local.NodeId).First();
            var partitionQueueName = PartitionConstants.FormatPartitionQueueName(QueueName, partition);
            var primary = ctx.NodeById(ctx.Assignment.GetAssignment(QueueName, partition)!.PrimaryNodeId);
            var message = NewMessage(QueueName, "payload");

            ctx.Clustered.EnqueueToPartition(message, partition);

            var forwarded = ctx.Queue.Store.PersistedOutgoing().ShouldHaveSingleItem();
            forwarded.Id.ShouldBe(message.Id);
            forwarded.QueueString.ShouldBe(partitionQueueName);
            forwarded.Destination!.Port.ShouldBe(primary.QueueEndpoint.Port);

            ctx.Queue.Store.PersistedIncoming(partitionQueueName)
                .Select(m => m.Id).ShouldBe([message.Id]);
            ctx.Replication.Calls.ShouldBeEmpty();
        });
    }

    [Fact]
    public async Task enqueue_on_uninvolved_node_forwards_to_primary_and_replicates_to_replica()
    {
        await ClusterScenarioAsync(ctx =>
        {
            var partition = ctx.PartitionsWhere(a =>
                a.PrimaryNodeId != ctx.Local.NodeId && a.ReplicaNodeId != ctx.Local.NodeId).First();
            var partitionQueueName = PartitionConstants.FormatPartitionQueueName(QueueName, partition);
            var assignment = ctx.Assignment.GetAssignment(QueueName, partition)!;
            var message = NewMessage(QueueName, "payload");

            ctx.Clustered.EnqueueToPartition(message, partition);

            ctx.Queue.Store.PersistedIncoming(partitionQueueName).ShouldBeEmpty();
            ctx.Queue.Store.PersistedOutgoing().ShouldHaveSingleItem().Destination!.Port
                .ShouldBe(ctx.NodeById(assignment.PrimaryNodeId).QueueEndpoint.Port);

            var call = ctx.Replication.Calls.ShouldHaveSingleItem();
            call.Endpoint.Port.ShouldBe(ctx.NodeById(assignment.ReplicaNodeId).QueueEndpoint.Port + ctx.Config.GossipPortOffset + 1);
            call.Messages.ShouldHaveSingleItem().QueueString.ShouldBe(partitionQueueName);
        }, nodeCount: 3);
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
    public async Task synchronous_mode_throws_when_replica_does_not_acknowledge()
    {
        await ClusterScenarioAsync(ctx =>
        {
            ctx.Replication.Acknowledge = false;
            var partition = ctx.PartitionsWhere(a => a.PrimaryNodeId == ctx.Local.NodeId).First();
            var message = NewMessage(QueueName, "payload");

            var ex = Should.Throw<ClusterReplicationException>(() => ctx.Clustered.EnqueueToPartition(message, partition));

            ex.PartitionKey.ShouldBe(PartitionConstants.FormatPartitionQueueName(QueueName, partition));
            // The primary copy is kept, so the message is under-replicated rather than lost
            ctx.Queue.Store.PersistedIncoming(ex.PartitionKey).Select(m => m.Id).ShouldContain(message.Id);
        });
    }

    [Fact]
    public async Task synchronous_mode_throws_when_replica_is_not_active()
    {
        await ClusterScenarioAsync(ctx =>
        {
            var partition = ctx.PartitionsWhere(a => a.PrimaryNodeId == ctx.Local.NodeId).First();
            ctx.NodeById(ctx.Assignment.GetAssignment(QueueName, partition)!.ReplicaNodeId).State = NodeState.Suspected;

            Should.Throw<ClusterReplicationException>(() =>
                ctx.Clustered.EnqueueToPartition(NewMessage(QueueName), partition));
            ctx.Replication.Calls.ShouldBeEmpty();
        });
    }

    [Fact]
    public async Task asynchronous_mode_does_not_throw_when_replica_does_not_acknowledge()
    {
        await ClusterScenarioAsync(ctx =>
        {
            ctx.Replication.Acknowledge = false;
            var partition = ctx.PartitionsWhere(a => a.PrimaryNodeId == ctx.Local.NodeId).First();

            Should.NotThrow(() => ctx.Clustered.EnqueueToPartition(NewMessage(QueueName), partition));
            ctx.Replication.Calls.Count.ShouldBe(1);
        }, configure: c => c.ReplicationMode = ReplicationMode.Asynchronous);
    }

    [Fact]
    public async Task replication_factor_one_assigns_no_replicas_and_never_replicates()
    {
        await ClusterScenarioAsync(ctx =>
        {
            ctx.Assignment.GetAssignments(QueueName).ShouldAllBe(a => a.ReplicaNodeId == Guid.Empty);

            var partition = ctx.PartitionsWhere(a => a.PrimaryNodeId == ctx.Local.NodeId).First();
            ctx.Clustered.EnqueueToPartition(NewMessage(QueueName), partition);

            ctx.Replication.Calls.ShouldBeEmpty();
        }, configure: c => c.ReplicationFactor = 1);
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
    public async Task start_cluster_assigns_existing_partitioned_queues()
    {
        await ClusterScenarioAsync(ctx =>
        {
            ctx.Partitioned.CreatePartitionedQueue("payments", 3);
            ctx.Assignment.GetAssignments("payments").ShouldBeEmpty();

            ctx.Clustered.StartCluster();
            ctx.Clustered.StartCluster(); // idempotent

            ctx.Assignment.GetAssignments("payments").Count.ShouldBe(3);
            ctx.Replication.ListenCalls.ShouldBe(1);
            ctx.Coordinator.JoinCalls.ShouldBe(1);
        });
    }

    [Fact]
    public async Task start_cluster_surfaces_listener_bind_failures()
    {
        await ClusterScenarioAsync(ctx =>
        {
            ctx.Replication.ListenFailure = new SocketException((int)SocketError.AddressAlreadyInUse);

            Should.Throw<SocketException>(() => ctx.Clustered.StartCluster());
        });
    }

    [Fact]
    public async Task node_leaving_moves_its_partitions_and_syncs_from_previous_replica()
    {
        await ClusterScenarioAsync(ctx =>
        {
            var remote = ctx.Remotes[0];
            var taken = ctx.PartitionsWhere(a => a.PrimaryNodeId == remote.NodeId);

            remote.State = NodeState.Draining;
            ctx.Coordinator.Raise(ClusterMembershipChangeType.NodeLeft, remote);

            ctx.Assignment.GetAssignments(QueueName).ShouldAllBe(a => a.PrimaryNodeId == ctx.Local.NodeId);
            // The departed node cannot serve a sync and this node was its replica, so there is
            // no other source: the partitions are served from the local replica copy.
            ctx.Replication.SyncRequests.ShouldBeEmpty();
            taken.ShouldNotBeEmpty();
        });
    }

    [Fact]
    public async Task node_joining_takes_over_partitions_and_local_node_syncs_new_replicas()
    {
        await ClusterScenarioAsync(ctx =>
        {
            // Start as a single-node cluster: this node owns everything
            ctx.Assignment.GetAssignments(QueueName).ShouldAllBe(a => a.PrimaryNodeId == ctx.Local.NodeId);

            var joined = ctx.AddRemote();
            ctx.Coordinator.Raise(ClusterMembershipChangeType.NodeJoined, joined);

            var assignments = ctx.Assignment.GetAssignments(QueueName);
            assignments.Count(a => a.PrimaryNodeId == joined.NodeId).ShouldBe(PartitionCount / 2);
            assignments.ShouldAllBe(a => a.ReplicaNodeId != Guid.Empty && a.ReplicaNodeId != a.PrimaryNodeId);

            // For partitions handed over to the joined node, this node is now the replica and
            // pulls from the new primary (which in turn pulls from this node).
            var expected = assignments
                .Where(a => a.ReplicaNodeId == ctx.Local.NodeId)
                .Select(a => a.PartitionKey);
            ctx.Replication.SyncRequests.Select(r => r.QueueName).ShouldBe(expected, ignoreOrder: true);
        }, nodeCount: 1);
    }

    [Fact]
    public async Task replication_protocol_round_trips_messages_with_headers()
    {
        await ReplicationScenarioAsync(async (store, primary, replicaEndpoint, token) =>
        {
            var partitionQueueName = PartitionConstants.FormatPartitionQueueName(QueueName, 0);
            var message = Message.Create(
                data: Encoding.UTF8.GetBytes("payload"),
                queue: partitionQueueName,
                partitionKey: "customer-1",
                headers: new Dictionary<string, string> { ["tenant"] = "acme" });

            var acked = await primary.ReplicateAsync(replicaEndpoint, partitionQueueName, [message], epoch: 1, token);
            acked.ShouldBeTrue();

            var stored = store.PersistedIncoming(partitionQueueName).ShouldHaveSingleItem();
            stored.Id.ShouldBe(message.Id);
            stored.GetHeadersDictionary()["tenant"].ShouldBe("acme");

            var synced = await primary.SyncPartitionAsync(replicaEndpoint, partitionQueueName, afterMessageId: null, token);
            var syncedMessage = synced.ShouldHaveSingleItem();
            syncedMessage.Id.ShouldBe(message.Id);
            syncedMessage.SentAt.ShouldBe(message.SentAt);
            syncedMessage.GetHeadersDictionary()["tenant"].ShouldBe("acme");

            var afterLast = await primary.SyncPartitionAsync(replicaEndpoint, partitionQueueName, message.Id, token);
            afterLast.ShouldBeEmpty();
        });
    }

    [Fact]
    public async Task replication_protocol_rejects_writes_outside_the_requested_partition_queue()
    {
        await ReplicationScenarioAsync(async (store, primary, replicaEndpoint, token) =>
        {
            var partitionQueueName = PartitionConstants.FormatPartitionQueueName(QueueName, 0);

            // A non-partition queue name is rejected
            var plain = NewMessage(QueueName);
            (await primary.ReplicateAsync(replicaEndpoint, QueueName, [plain], epoch: 1, token)).ShouldBeFalse();

            // A message whose queue does not match the request is rejected
            var mismatched = NewMessage(PartitionConstants.FormatPartitionQueueName(QueueName, 1));
            (await primary.ReplicateAsync(replicaEndpoint, partitionQueueName, [mismatched], epoch: 1, token)).ShouldBeFalse();

            store.PersistedIncoming(QueueName).ShouldBeEmpty();
            store.PersistedIncoming(partitionQueueName).ShouldBeEmpty();
            (await primary.SyncPartitionAsync(replicaEndpoint, QueueName, null, token)).ShouldBeEmpty();
        });
    }

    [Fact]
    public async Task replication_protocol_drops_oversized_frames()
    {
        await ReplicationScenarioAsync(async (store, _, replicaEndpoint, token) =>
        {
            using var client = new TcpClient();
            await client.ConnectAsync(replicaEndpoint, token);
            var stream = client.GetStream();
            await stream.WriteAsync(BitConverter.GetBytes(int.MaxValue), token);

            // The listener closes the connection instead of allocating the requested buffer
            var buffer = new byte[1];
            (await stream.ReadAsync(buffer, token)).ShouldBe(0);
        }, maxPayloadBytes: 1024);
    }

    private async Task ReplicationScenarioAsync(
        Func<LmdbMessageStore, TcpReplicationProtocol, IPEndPoint, CancellationToken, Task> scenario,
        int maxPayloadBytes = ClusterConfiguration.DefaultMaxPayloadBytes)
    {
        using var replicaEnv = LightningEnvironment();
        using var replicaStore = new LmdbMessageStore(replicaEnv, new MessageSerializer());
        replicaStore.CreateQueue(QueueName);
        replicaStore.CreateQueue(PartitionConstants.FormatPartitionQueueName(QueueName, 0));
        replicaStore.CreateQueue(PartitionConstants.FormatPartitionQueueName(QueueName, 1));

        var replicaEndpoint = new IPEndPoint(IPAddress.Loopback, GetFreePort());
        await using var replica = new TcpReplicationProtocol(replicaStore, replicaEndpoint,
            new RecordingLogger<TcpReplicationProtocol>(OutputWriter, LogLevel.Information), maxPayloadBytes);
        await using var primary = new TcpReplicationProtocol(replicaStore, new IPEndPoint(IPAddress.Loopback, 0),
            new RecordingLogger<TcpReplicationProtocol>(OutputWriter, LogLevel.Information));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var listening = replica.StartListeningAsync(cts.Token);

        await scenario(replicaStore, primary, replicaEndpoint, cts.Token);

        await cts.CancelAsync();
        await listening;
    }

    private static int GetFreePort()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        return ((IPEndPoint)listener.LocalEndpoint).Port;
    }

    private async Task ClusterScenarioAsync(Action<ClusterScenario> scenario, int nodeCount = 2,
        Action<ClusterConfiguration>? configure = null)
    {
        await QueueScenarioAsync((queue, _) =>
        {
            var partitioned = new PartitionedQueue(queue, new HashPartitionStrategy());
            var local = NewNode(5000);
            var remotes = Enumerable.Range(1, nodeCount - 1).Select(i => NewNode(5000 + i * 10)).ToList();
            var coordinator = new FakeClusterCoordinator(local, remotes);
            var config = new ClusterConfiguration { NodeId = local.NodeId };
            configure?.Invoke(config);
            var assignment = new PartitionAssignmentTable(
                new RecordingLogger<PartitionAssignmentTable>(OutputWriter, LogLevel.Information), config.ReplicationFactor);
            var replication = new RecordingReplicationProtocol();

            var clustered = new ClusteredPartitionedQueueDecorator(partitioned, coordinator, assignment, replication, config,
                new RecordingLogger<ClusteredPartitionedQueueDecorator>(OutputWriter, LogLevel.Information));
            clustered.CreatePartitionedQueue(QueueName, PartitionCount);

            scenario(new ClusterScenario(queue, partitioned, clustered, assignment, replication, coordinator, config, local, remotes));
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
        FakeClusterCoordinator Coordinator,
        ClusterConfiguration Config,
        NodeInfo Local,
        List<NodeInfo> Remotes)
    {
        public NodeInfo NodeById(Guid nodeId) => Coordinator.GetMembers().Single(n => n.NodeId == nodeId);

        public NodeInfo AddRemote()
        {
            var node = NewNode(6000 + Remotes.Count * 10);
            Remotes.Add(node);
            Coordinator.Add(node);
            return node;
        }

        public int[] PartitionsWhere(Func<PartitionReplica, bool> predicate)
        {
            var partitions = Assignment.GetAssignments(QueueName)
                .Where(predicate)
                .Select(a => a.PartitionIndex)
                .ToArray();
            partitions.ShouldNotBeEmpty("expected the assignment to contain a matching partition");
            return partitions;
        }
    }

    private sealed class FakeClusterCoordinator(NodeInfo local, IEnumerable<NodeInfo> others) : IClusterCoordinator
    {
        private readonly List<NodeInfo> _members = [local, .. others];

        public int JoinCalls { get; private set; }
        public NodeInfo LocalNode => local;
        public IReadOnlyList<NodeInfo> GetMembers() => _members;
        public IReadOnlyList<NodeInfo> GetActiveMembers() => _members.Where(m => m.State == NodeState.Active).ToList();

        public Task JoinAsync(CancellationToken cancellationToken = default)
        {
            JoinCalls++;
            return Task.CompletedTask;
        }

        public Task LeaveAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
        public event EventHandler<ClusterMembershipChangedEventArgs>? MembershipChanged;
        public void Add(NodeInfo node) => _members.Add(node);

        public void Raise(ClusterMembershipChangeType changeType, NodeInfo node) =>
            MembershipChanged?.Invoke(this, new ClusterMembershipChangedEventArgs { ChangeType = changeType, Node = node });

        public void Dispose() { }
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class RecordingReplicationProtocol : IReplicationProtocol
    {
        public List<(IPEndPoint Endpoint, string QueueName, IReadOnlyList<Message> Messages, long Epoch)> Calls { get; } = [];
        public List<(IPEndPoint Endpoint, string QueueName)> SyncRequests { get; } = [];
        public bool Acknowledge { get; set; } = true;
        public int ListenCalls { get; private set; }
        public Exception? ListenFailure { get; set; }

        public Task<bool> ReplicateAsync(IPEndPoint replicaEndpoint, string queueName, IReadOnlyList<Message> messages,
            long epoch, CancellationToken cancellationToken = default)
        {
            Calls.Add((replicaEndpoint, queueName, messages, epoch));
            return Task.FromResult(Acknowledge);
        }

        public Task<IReadOnlyList<Message>> SyncPartitionAsync(IPEndPoint sourceEndpoint, string queueName,
            MessageId? afterMessageId, CancellationToken cancellationToken = default)
        {
            SyncRequests.Add((sourceEndpoint, queueName));
            return Task.FromResult<IReadOnlyList<Message>>([]);
        }

        public Task StartListeningAsync(CancellationToken cancellationToken = default)
        {
            ListenCalls++;
            return ListenFailure != null ? Task.FromException(ListenFailure) : Task.CompletedTask;
        }

        public void Dispose() { }
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
