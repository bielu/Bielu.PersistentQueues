---
"bielu-persistentqueues": major
---

First stable release.

Everything below shipped previously only to the `0.6.0-beta.*` pre-release channel on NuGet.org,
starting from the March 2026 fork of `LightningQueues` into `Bielu.PersistentQueues`. This entry
promotes that work to the first stable release.

  **Storage.** Split storage out of the core package into `Bielu.PersistentQueues.Storage.LMDB`
  (LightningDB-backed, with Microsoft.Extensions.DependencyInjection integration) and added a second
  pluggable backend, `Bielu.PersistentQueues.Storage.ZoneTree`. Added `StorageSize`, a human-readable
  type for configuring LMDB's `MapSize`.

  **Partitioning.** Added Kafka-like queue partitioning: `EnablePartitioning`, `DisablePartitioning`,
  and `Repartition` APIs. `Enqueue` now handles both partitioned and non-partitioned queues through
  one method, and `GetPartitionMessageCount` lets high-partition-count scenarios skip empty
  partitions.

  **Messaging.** Added a dead letter queue with processing-attempt tracking, and strongly-typed
  message content via a pluggable serializer and typed queue extension methods.

  **Observability** (`Bielu.PersistentQueues.OpenTelemetry`). Added a storage-usage-percentage
  metric, a queue-depth observable gauge, a time-in-queue histogram, and a per-partition depth gauge
  with a partition sub-tag on the shared metrics.

  **Tooling.** Added a benchmark strategy with weekly performance tracking and PR benchmark
  comments, and adopted the Bielu static analyzer across the suite.

This entry also exists to pin the version arithmetic. The shared-version placeholder
(`build/changeset/nuget-suite`) is held at `0.0.0` and this major bump lands it exactly on `1.0.0`
— without it, `changeset version` would resolve the next version from `0.0.0` using whatever bump
levels land in later changesets, which is not guaranteed to be `1.0.0`. Landing this changeset alone
in the migration PR keeps the arithmetic unambiguous: the generated Version Packages PR moves the
placeholder `0.0.0 -> 1.0.0`, writes `1.0.0` into `version.props`, and merging it publishes the
first stable `Bielu.PersistentQueues` suite at `1.0.0`.
