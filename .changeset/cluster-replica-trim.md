---
"bielu-persistentqueues": minor
---

Cluster: primaries now periodically remove replica copies of messages they have consumed, so replica partitions no longer grow without limit and a failover no longer redelivers the whole partition history. Configure it with `ClusterConfiguration.ReplicaTrimInterval` and `ReplicaTrimGracePeriod`. `IReplicationProtocol` gains `ListReplicaMessageIdsAsync` and `TrimReplicaAsync`.
