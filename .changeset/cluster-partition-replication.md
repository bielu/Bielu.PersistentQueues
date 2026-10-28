---
"bielu-persistentqueues": minor
---

Add the `Bielu.PersistentQueues.Cluster` package: gossip-based cluster membership, per-partition primary/replica assignment with epochs, TCP replication (synchronous or asynchronous) and automatic replica promotion on node failure, enabled via `AddClusterSupport(...)` on top of `UsePartitioning(...)`. `IMessageStore` gains a default `GetMessagesAfter(queueName, afterMessageId)` method used for catch-up replication.
