# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and from now on the
`[Unreleased]` section and released version entries are managed with
[changesets](https://github.com/changesets/changesets) — see [`.changeset/README.md`](.changeset/README.md).

`Bielu.PersistentQueues` is a fork of [LightningQueues](https://github.com/LightningQueues/LightningQueues),
originally created by [Corey Kaylor](https://github.com/CoreyKaylor). Below you'll find the version
history for the `Bielu.*` package suite, plus the legacy `LightningQueues` lineage it forked from.

> **Versioning note.** No stable release has shipped yet under the `Bielu.PersistentQueues` name.
> Every package in the current suite (`Bielu.PersistentQueues`, `Bielu.PersistentQueues.Storage.LMDB`,
> `Bielu.PersistentQueues.Storage.ZoneTree`, `Bielu.PersistentQueues.OpenTelemetry`) shares a single
> version (`version.props`) and has so far only been published to the **`beta` pre-release channel**
> on NuGet.org (`0.6.0-beta.*`). The [Pre-Changesets beta era](#pre-changesets-beta-era) section below
> accounts for that prerelease history; the itemized changes it shipped are folded into the `1.0.0`
> entry once this migration's bootstrap changeset is versioned.

## [Unreleased]

## [2.0.0] - 2026-10-29

### Major Changes

- [#90](https://github.com/bielu/Bielu.PersistentQueues/pull/90) [`d8abac1`](https://github.com/bielu/Bielu.PersistentQueues/commit/d8abac1628da19933396f6ae3ed2b583c5614c1d) Thanks [@bielu](https://github.com/bielu)! - Upgrade `ZoneTree` 1.9.8 → 2.0.5 (`Bielu.PersistentQueues.Storage.ZoneTree`) and `LightningDB` 0.23.0 → 0.23.1 (`Bielu.PersistentQueues.Storage.LMDB`).

  ZoneTree 2.x is faster (about 1.48× across its reference benchmarks) and fixes crash-recovery, compressed-WAL repair and seek bugs. LightningDB 0.23.1 fixes a process crash on the finalizer thread when opening a cursor or transaction fails.

  **Breaking (on-disk):** ZoneTree 2.0 switches WAL checksums from CRC32 to XXH3 and renames existing WAL files the first time a store is opened. Data written by 1.x is migrated automatically and reads back intact, but the migration is one-way: rolling back to a 1.x ZoneTree provider afterwards silently loses records that were still only in the WAL. Back up the data directory before upgrading if a rollback might be needed. The public API is unchanged.

### Patch Changes

- [#81](https://github.com/bielu/Bielu.PersistentQueues/pull/81) [`c2a66a8`](https://github.com/bielu/Bielu.PersistentQueues/commit/c2a66a830820b59fcea7b82ba439ef71d2bce16e) Thanks [@bielu](https://github.com/bielu)! - Publish NuGet packages through nuget.org trusted publishing from this repository's own workflow (the previous stable release failed because the OIDC token was minted inside the shared template), skip beta releases for pushes that change no shipped code, and align repository tooling with Bielu.AspNetCore.AsyncApi: `.editorconfig`, shared `coverlet.runsettings`, `AGENTS.md`, and a daily ClearlyDefined harvest.

- [#81](https://github.com/bielu/Bielu.PersistentQueues/pull/81) [`c2a66a8`](https://github.com/bielu/Bielu.PersistentQueues/commit/c2a66a830820b59fcea7b82ba439ef71d2bce16e) Thanks [@bielu](https://github.com/bielu)! - Dependency updates: `ZoneTree` 1.9.7 → 1.9.8 (`Bielu.PersistentQueues.Storage.ZoneTree`) and `OpenTelemetry.Api` 1.18.0 → 1.19.1 (`Bielu.PersistentQueues.OpenTelemetry`).

- [#83](https://github.com/bielu/Bielu.PersistentQueues/pull/83) [`fe394d8`](https://github.com/bielu/Bielu.PersistentQueues/commit/fe394d8c2fadb9977112c36a298053583239a367) Thanks [@bielu](https://github.com/bielu)! - `Message.WithBytes(...)` is now `[Obsolete]`, and the `bytes` parameter of the `Message` constructor is documented as ignored. `Message` stopped storing raw serialized bytes in an earlier upstream change, so both have been no-ops since then. They stay for 1.x binary compatibility and will be removed in the next major version.

## [1.0.0] - 2026-09-12

### Major Changes

- [#60](https://github.com/bielu/Bielu.PersistentQueues/pull/60) [`1340e39`](https://github.com/bielu/Bielu.PersistentQueues/commit/1340e3970efe8bcb9e5163e04806e5423856152b) Thanks [@bielu](https://github.com/bielu)! - First stable release.

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

## Pre-Changesets beta era

Before this repository adopted changesets, every push to `main` published a fresh
`0.6.0-beta.<UTC ticks>` prerelease of all four `Bielu.*` packages directly from the static version
in `version.props`, without individually tracked release notes. NuGet's flat-container API recorded
40+ such prereleases per package (checked 2026-08-08), spanning **2026-03-24 through 2026-08-08**:

| Package | Prereleases (as of 2026-08-08) |
|---|---:|
| `Bielu.PersistentQueues` | 40 |
| `Bielu.PersistentQueues.Storage.LMDB` | 40 |
| `Bielu.PersistentQueues.Storage.ZoneTree` | 10 |
| `Bielu.PersistentQueues.OpenTelemetry` | 35 |

This train established the current package suite and its shared-version model: the core
`Bielu.PersistentQueues` package plus three extensions (`Storage.LMDB`, `Storage.ZoneTree`,
`OpenTelemetry`) that all depend on it and version together. No package in this train ever reached a
stable release — see the **1.0.0** entry above (once versioned) for the itemized features and fixes
this train shipped.

## Legacy LightningQueues lineage

`Bielu.PersistentQueues` began as a fork of `LightningQueues`, which shipped stable releases through
`0.6.0` under that name. These are documented for lineage, not as `Bielu.*` releases:

- **0.6.0** — [`2acc3ea`](https://github.com/bielu/Bielu.PersistentQueues/commit/2acc3ea), 2025-12-24
- **0.5.0 alpha preparation** — [`770f38e`](https://github.com/bielu/Bielu.PersistentQueues/commit/770f38e), 2025-02-27
- **0.4.0** — [`2a40a62`](https://github.com/bielu/Bielu.PersistentQueues/commit/2a40a62), 2021-01-15 — LightningDB update.
- **0.3.1** — [`d9ebf2d`](https://github.com/bielu/Bielu.PersistentQueues/commit/d9ebf2d), 2020-03-20
- **0.3.0** — [`f7726b9`](https://github.com/bielu/Bielu.PersistentQueues/commit/f7726b9), 2020-02-26

See the [`LightningQueues`](https://api.nuget.org/v3-flatcontainer/lightningqueues/index.json) NuGet
flat-container index for the full published history under that identity.
