---
"bielu-persistentqueues": minor
---

Cluster: add optional peer authentication. Set `ClusterConfiguration.SharedSecret` to the same value on every node, and gossip and replication frames are then signed with HMAC-SHA256 and a timestamp. Unsigned, forged or stale frames are dropped. Replicas also accept trims only from the node they see as the partition's primary. A new `TcpReplicationProtocol(store, endpoint, config, assignment, logger)` constructor enables both, and `AddClusterSupport` uses it.
