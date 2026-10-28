using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Bielu.PersistentQueues.Cluster;
using Bielu.PersistentQueues.Storage;
using Microsoft.Extensions.Logging;
using Shouldly;
using Xunit;
using Xunit.Abstractions;

namespace Bielu.PersistentQueues.Tests;

public class PartitionAssignmentTableTests
{
    private readonly ILogger<PartitionAssignmentTable> _logger;

    public PartitionAssignmentTableTests(ITestOutputHelper output)
    {
        _logger = new Bielu.PersistentQueues.Logging.RecordingLogger<PartitionAssignmentTable>(
            new TestOutputHelperWriter(output), LogLevel.Information);
    }

    private static List<NodeInfo> CreateNodes(int count)
    {
        var nodes = new List<NodeInfo>();
        for (int i = 0; i < count; i++)
        {
            nodes.Add(new NodeInfo
            {
                NodeId = Guid.NewGuid(),
                QueueEndpoint = new IPEndPoint(IPAddress.Loopback, 5000 + i),
                GossipEndpoint = new IPEndPoint(IPAddress.Loopback, 6000 + i),
                State = NodeState.Active
            });
        }
        return nodes;
    }

    [Fact]
    public void assign_partitions_distributes_evenly_across_nodes()
    {
        var table = new PartitionAssignmentTable(_logger);
        var nodes = CreateNodes(3);

        table.AssignPartitions("orders", 6, nodes);

        var assignments = table.GetAssignments("orders");
        assignments.Count.ShouldBe(6);

        // Each node should be primary for 2 partitions (6 / 3)
        foreach (var node in nodes)
        {
            table.GetPrimaryPartitions(node.NodeId).Count.ShouldBe(2);
        }
    }

    [Fact]
    public void assign_partitions_assigns_different_primary_and_replica()
    {
        var table = new PartitionAssignmentTable(_logger);
        var nodes = CreateNodes(3);

        table.AssignPartitions("orders", 6, nodes);

        var assignments = table.GetAssignments("orders");
        foreach (var partition in assignments)
        {
            partition.IsFullyReplicated.ShouldBeTrue();
            partition.PrimaryNodeId.ShouldNotBe(partition.ReplicaNodeId);
        }
    }

    [Fact]
    public void assign_partitions_single_node_has_no_replica()
    {
        var table = new PartitionAssignmentTable(_logger);
        var nodes = CreateNodes(1);

        table.AssignPartitions("orders", 4, nodes);

        var assignments = table.GetAssignments("orders");
        foreach (var partition in assignments)
        {
            partition.PrimaryNodeId.ShouldBe(nodes[0].NodeId);
            partition.ReplicaNodeId.ShouldBe(Guid.Empty);
            partition.IsFullyReplicated.ShouldBeFalse();
        }
    }

    [Fact]
    public void assign_partitions_zero_nodes_throws()
    {
        var table = new PartitionAssignmentTable(_logger);
        Should.Throw<InvalidOperationException>(() =>
            table.AssignPartitions("orders", 4, new List<NodeInfo>()));
    }

    [Fact]
    public void get_assignment_returns_correct_partition()
    {
        var table = new PartitionAssignmentTable(_logger);
        var nodes = CreateNodes(2);

        table.AssignPartitions("orders", 4, nodes);

        var partition2 = table.GetAssignment("orders", 2);
        partition2.ShouldNotBeNull();
        partition2.PartitionIndex.ShouldBe(2);
        partition2.QueueName.ShouldBe("orders");
    }

    [Fact]
    public void get_assignment_unknown_queue_returns_null()
    {
        var table = new PartitionAssignmentTable(_logger);
        table.GetAssignment("nonexistent", 0).ShouldBeNull();
    }

    [Fact]
    public void epoch_increments_on_assignment()
    {
        var table = new PartitionAssignmentTable(_logger);
        var nodes = CreateNodes(2);

        table.AssignPartitions("orders", 4, nodes);

        foreach (var partition in table.GetAssignments("orders"))
        {
            partition.Epoch.ShouldBe(1);
        }
    }

    [Fact]
    public void promote_replica_swaps_primary_and_increments_epoch()
    {
        var table = new PartitionAssignmentTable(_logger);
        var nodes = CreateNodes(2);

        table.AssignPartitions("orders", 4, nodes);

        var before = table.GetAssignment("orders", 0)!;
        var oldPrimary = before.PrimaryNodeId;
        var oldReplica = before.ReplicaNodeId;
        var oldEpoch = before.Epoch;

        var promoted = table.PromoteReplica("orders", 0);

        promoted.ShouldNotBeNull();
        promoted.PrimaryNodeId.ShouldBe(oldReplica);
        promoted.ReplicaNodeId.ShouldBe(Guid.Empty); // No replica after promotion
        promoted.Epoch.ShouldBe(oldEpoch + 1);
    }

    [Fact]
    public void promote_replica_no_replica_returns_null()
    {
        var table = new PartitionAssignmentTable(_logger);
        var nodes = CreateNodes(1);

        table.AssignPartitions("orders", 2, nodes);

        // Single node = no replica
        table.PromoteReplica("orders", 0).ShouldBeNull();
    }

    [Fact]
    public void rebalance_after_node_death_promotes_replica()
    {
        var table = new PartitionAssignmentTable(_logger);
        var nodes = CreateNodes(3);

        table.AssignPartitions("orders", 6, nodes);

        // "Kill" node 0 — remove from active list
        var deadNodeId = nodes[0].NodeId;
        var activeNodes = nodes.Skip(1).ToList();

        table.Rebalance(activeNodes);

        // No partition should have the dead node as primary
        var assignments = table.GetAssignments("orders");
        foreach (var partition in assignments)
        {
            partition.PrimaryNodeId.ShouldNotBe(deadNodeId);
        }
    }

    [Fact]
    public void rebalance_reassigns_replicas_for_dead_node()
    {
        var table = new PartitionAssignmentTable(_logger);
        var nodes = CreateNodes(3);

        table.AssignPartitions("orders", 6, nodes);

        var deadNodeId = nodes[0].NodeId;
        var activeNodes = nodes.Skip(1).ToList();

        table.Rebalance(activeNodes);

        // All partitions should have replicas assigned to active nodes
        var activeNodeIds = new HashSet<Guid>(activeNodes.Select(n => n.NodeId));
        var assignments = table.GetAssignments("orders");
        foreach (var partition in assignments)
        {
            activeNodeIds.ShouldContain(partition.PrimaryNodeId);
            // With 2 active nodes, replicas should be the other node
            if (partition.ReplicaNodeId != Guid.Empty)
            {
                activeNodeIds.ShouldContain(partition.ReplicaNodeId);
                partition.ReplicaNodeId.ShouldNotBe(partition.PrimaryNodeId);
            }
        }
    }

    [Fact]
    public void partition_key_format_is_correct()
    {
        var table = new PartitionAssignmentTable(_logger);
        var nodes = CreateNodes(2);

        table.AssignPartitions("orders", 3, nodes);

        var partition = table.GetAssignment("orders", 1)!;
        partition.PartitionKey.ShouldBe("orders:partition-1");
    }

    [Fact]
    public void assignment_changed_event_fires_on_assign()
    {
        var table = new PartitionAssignmentTable(_logger);
        var nodes = CreateNodes(2);
        var eventFired = false;

        table.AssignmentChanged += (_, args) =>
        {
            eventFired = true;
            args.ChangedPartitions.Count.ShouldBe(4);
        };

        table.AssignPartitions("orders", 4, nodes);
        eventFired.ShouldBeTrue();
    }

    [Fact]
    public void assignment_changed_event_fires_on_promote()
    {
        var table = new PartitionAssignmentTable(_logger);
        var nodes = CreateNodes(2);
        table.AssignPartitions("orders", 4, nodes);

        var eventFired = false;
        table.AssignmentChanged += (_, args) =>
        {
            eventFired = true;
            args.ChangedPartitions.Count.ShouldBe(1);
        };

        table.PromoteReplica("orders", 0);
        eventFired.ShouldBeTrue();
    }

    [Fact]
    public void assignment_is_independent_of_node_order()
    {
        var nodes = CreateNodes(3);
        var first = new PartitionAssignmentTable(_logger);
        var second = new PartitionAssignmentTable(_logger);

        first.AssignPartitions("orders", 6, nodes);
        second.AssignPartitions("orders", 6, nodes.AsEnumerable().Reverse().ToList());

        second.GetAssignments("orders").Select(a => (a.PrimaryNodeId, a.ReplicaNodeId))
            .ShouldBe(first.GetAssignments("orders").Select(a => (a.PrimaryNodeId, a.ReplicaNodeId)));
    }

    [Fact]
    public void rebalance_converges_regardless_of_history()
    {
        var nodes = CreateNodes(2);

        // Each node starts alone (before gossip found its peer) and assigns everything to itself
        var first = new PartitionAssignmentTable(_logger);
        var second = new PartitionAssignmentTable(_logger);
        first.AssignPartitions("orders", 4, [nodes[0]]);
        second.AssignPartitions("orders", 4, [nodes[1]]);

        first.Rebalance(nodes);
        second.Rebalance(nodes);

        second.GetAssignments("orders").Select(a => (a.PrimaryNodeId, a.ReplicaNodeId))
            .ShouldBe(first.GetAssignments("orders").Select(a => (a.PrimaryNodeId, a.ReplicaNodeId)));
        foreach (var node in nodes)
            first.GetPrimaryPartitions(node.NodeId).Count.ShouldBe(2);
    }

    [Fact]
    public void rebalance_increments_epoch_only_when_primary_moves()
    {
        var table = new PartitionAssignmentTable(_logger);
        var nodes = CreateNodes(2);
        table.AssignPartitions("orders", 4, [nodes[0]]);
        var before = table.GetAssignments("orders").ToDictionary(a => a.PartitionIndex, a => a.PrimaryNodeId);

        table.Rebalance(nodes);

        foreach (var partition in table.GetAssignments("orders"))
            partition.Epoch.ShouldBe(before[partition.PartitionIndex] == partition.PrimaryNodeId ? 1 : 2);
    }

    [Fact]
    public void replication_factor_one_assigns_no_replica()
    {
        var table = new PartitionAssignmentTable(_logger, replicationFactor: 1);

        table.AssignPartitions("orders", 4, CreateNodes(3));

        table.GetAssignments("orders").ShouldAllBe(a => a.ReplicaNodeId == Guid.Empty);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(3)]
    public void unsupported_replication_factor_throws(int replicationFactor)
    {
        Should.Throw<ArgumentOutOfRangeException>(() => new PartitionAssignmentTable(_logger, replicationFactor));
    }
}

public class NodeInfoTests
{
    [Fact]
    public void node_info_equality_by_node_id()
    {
        var id = Guid.NewGuid();
        var node1 = new NodeInfo
        {
            NodeId = id,
            QueueEndpoint = new IPEndPoint(IPAddress.Loopback, 5000),
            GossipEndpoint = new IPEndPoint(IPAddress.Loopback, 5001)
        };
        var node2 = new NodeInfo
        {
            NodeId = id,
            QueueEndpoint = new IPEndPoint(IPAddress.Loopback, 6000),
            GossipEndpoint = new IPEndPoint(IPAddress.Loopback, 6001)
        };

        node1.Equals(node2).ShouldBeTrue();
        node1.GetHashCode().ShouldBe(node2.GetHashCode());
    }

    [Fact]
    public void node_info_defaults()
    {
        var node = new NodeInfo
        {
            NodeId = Guid.NewGuid(),
            QueueEndpoint = new IPEndPoint(IPAddress.Loopback, 5000),
            GossipEndpoint = new IPEndPoint(IPAddress.Loopback, 5001)
        };

        node.State.ShouldBe(NodeState.Active);
        node.Generation.ShouldBe(0);
        node.Metadata.ShouldNotBeNull();
        node.Metadata.Count.ShouldBe(0);
    }
}

public class ClusterConfigurationTests
{
    [Fact]
    public void default_values()
    {
        var config = new ClusterConfiguration();

        config.ReplicationFactor.ShouldBe(2);
        config.ReplicationMode.ShouldBe(ReplicationMode.Synchronous);
        config.HeartbeatInterval.ShouldBe(TimeSpan.FromSeconds(2));
        config.SuspicionThreshold.ShouldBe(3);
        config.DeathThreshold.ShouldBe(2);
        config.GossipPortOffset.ShouldBe(1);
        config.NodeId.ShouldNotBe(Guid.Empty);
        config.SeedNodes.ShouldNotBeNull();
        config.SeedNodes.Count.ShouldBe(0);
    }

    [Fact]
    public void configuration_can_be_customized()
    {
        var nodeId = Guid.NewGuid();
        var config = new ClusterConfiguration
        {
            NodeId = nodeId,
            ReplicationFactor = 1,
            ReplicationMode = ReplicationMode.Asynchronous,
            HeartbeatInterval = TimeSpan.FromSeconds(5),
            SuspicionThreshold = 5,
            DeathThreshold = 3,
            GossipPortOffset = 10,
            SeedNodes = { new IPEndPoint(IPAddress.Parse("10.0.0.1"), 5000) }
        };

        config.NodeId.ShouldBe(nodeId);
        config.ReplicationFactor.ShouldBe(1);
        config.ReplicationMode.ShouldBe(ReplicationMode.Asynchronous);
        config.SeedNodes.Count.ShouldBe(1);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(3)]
    public void validate_rejects_unsupported_replication_factor(int replicationFactor)
    {
        var config = new ClusterConfiguration { ReplicationFactor = replicationFactor };

        Should.Throw<ArgumentOutOfRangeException>(config.Validate);
    }

    [Fact]
    public void validate_rejects_non_positive_payload_limit()
    {
        var config = new ClusterConfiguration { MaxPayloadBytes = 0 };

        Should.Throw<ArgumentOutOfRangeException>(config.Validate);
    }

    [Fact]
    public void validate_accepts_defaults()
    {
        Should.NotThrow(new ClusterConfiguration().Validate);
    }

    [Theory]
    [InlineData("0.0.0.0")]
    [InlineData("255.255.255.255")]
    [InlineData("::1")]
    public void validate_rejects_unroutable_or_non_ipv4_advertised_address(string address)
    {
        var config = new ClusterConfiguration { AdvertisedAddress = IPAddress.Parse(address) };

        Should.Throw<ArgumentException>(config.Validate).ParamName.ShouldBe(nameof(ClusterConfiguration.AdvertisedAddress));
    }

    [Fact]
    public void coordinator_advertises_configured_address_instead_of_bind_address()
    {
        var config = new ClusterConfiguration { AdvertisedAddress = IPAddress.Parse("10.0.0.5") };

        using var coordinator = new GossipClusterCoordinator(config, new IPEndPoint(IPAddress.Any, 5000),
            Microsoft.Extensions.Logging.Abstractions.NullLogger<GossipClusterCoordinator>.Instance);

        coordinator.LocalNode.QueueEndpoint.ShouldBe(new IPEndPoint(IPAddress.Parse("10.0.0.5"), 5000));
        coordinator.LocalNode.GossipEndpoint.ShouldBe(new IPEndPoint(IPAddress.Parse("10.0.0.5"), 5001));
    }

    [Theory]
    [InlineData("0.0.0.0")]
    [InlineData("::")]
    public void coordinator_rejects_unadvertisable_queue_address_without_advertised_address(string address)
    {
        Should.Throw<ArgumentException>(() => new GossipClusterCoordinator(new ClusterConfiguration(),
            new IPEndPoint(IPAddress.Parse(address), 5000),
            Microsoft.Extensions.Logging.Abstractions.NullLogger<GossipClusterCoordinator>.Instance));
    }
}

public class GetMessagesAfterTests(ITestOutputHelper output) : TestBase(output)
{
    [Fact]
    public void returns_messages_after_marker_even_when_marker_was_removed()
    {
        StorageScenario(store =>
        {
            var messages = Enumerable.Range(0, 4).Select(i => NewMessage(payload: $"m{i}")).ToList();
            store.StoreIncoming(messages);
            store.DeleteIncoming([messages[1]]);

            var after = ((IMessageStore)store).GetMessagesAfter("test", messages[1].Id).Select(m => m.Id).ToList();

            after.ShouldBe([messages[2].Id, messages[3].Id]);
        });
    }

    [Fact]
    public void returns_all_messages_without_marker()
    {
        StorageScenario(store =>
        {
            var messages = Enumerable.Range(0, 3).Select(i => NewMessage(payload: $"m{i}")).ToList();
            store.StoreIncoming(messages);

            ((IMessageStore)store).GetMessagesAfter("test", null).Count().ShouldBe(3);
        });
    }
}

public class PartitionReplicaTests
{
    [Fact]
    public void partition_key_uses_naming_convention()
    {
        var replica = new PartitionReplica
        {
            QueueName = "orders",
            PartitionIndex = 3,
            PrimaryNodeId = Guid.NewGuid(),
            ReplicaNodeId = Guid.NewGuid(),
            Epoch = 1
        };

        replica.PartitionKey.ShouldBe("orders:partition-3");
    }

    [Fact]
    public void is_fully_replicated_when_both_assigned()
    {
        var replica = new PartitionReplica
        {
            QueueName = "orders",
            PartitionIndex = 0,
            PrimaryNodeId = Guid.NewGuid(),
            ReplicaNodeId = Guid.NewGuid(),
            Epoch = 1
        };

        replica.IsFullyReplicated.ShouldBeTrue();
    }

    [Fact]
    public void is_not_fully_replicated_when_no_replica()
    {
        var replica = new PartitionReplica
        {
            QueueName = "orders",
            PartitionIndex = 0,
            PrimaryNodeId = Guid.NewGuid(),
            ReplicaNodeId = Guid.Empty,
            Epoch = 1
        };

        replica.IsFullyReplicated.ShouldBeFalse();
    }
}
