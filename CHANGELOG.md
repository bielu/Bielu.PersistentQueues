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
