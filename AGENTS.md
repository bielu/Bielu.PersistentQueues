# AGENTS.md

Guidance for AI coding agents working in this repository. Human contributors should also read [CONTRIBUTING.md](./CONTRIBUTING.md).

## What this project is

`Bielu.PersistentQueues` is a lightweight, high-performance **store-and-forward message queue** for .NET with **pluggable storage backends**. It is a fork of [LightningQueues](https://github.com/LightningQueues/LightningQueues) aligned with the bielu package ecosystem: strongly-typed messages, `Microsoft.Extensions.DependencyInjection` integration, dead-letter queues, partitioning, optional TLS transport and OpenTelemetry instrumentation.

## Repository layout

The solution is `src/Bielu.PersistentQueues.slnx`. Everything, tests included, lives under `src/`.

| Project | Purpose |
|---------|---------|
| `Bielu.PersistentQueues` | Core library: queue, contexts, messages, networking/transport, serialization, the `IMessageStore` storage abstraction and the DI builder (`PersistentQueuesBuilder`) |
| `Bielu.PersistentQueues.Storage.LMDB` | LMDB storage provider (`LmdbMessageStore`, over LightningDB) |
| `Bielu.PersistentQueues.Storage.ZoneTree` | ZoneTree storage provider (`ZoneTreeMessageStore`) |
| `Bielu.PersistentQueues.OpenTelemetry` | Tracing and metrics instrumentation |
| `Bielu.PersistentQueues.Tests` | xUnit test suite (not packed) |
| `Bielu.PersistentQueues.Benchmarks` | BenchmarkDotNet benchmarks; see [BENCHMARK_STRATEGY.md](./BENCHMARK_STRATEGY.md) (not packed) |
| `Bielu.PersistentQueues.Examples` | Runnable usage examples (not packed) |

Inside the core library, code is grouped by responsibility: `Core/` (queue, contexts, domain types), `Network/` (sender/receiver, protocol, TLS), `Storage/` (store abstractions), `Serialization/`, `Queue/`, `Infrastructure/`.

## Conventions

- **Target framework:** `net10.0`. Requires the **.NET 10 SDK**.
- **Language settings:** `Nullable` and `ImplicitUsings` enabled. Respect nullable annotations — don't suppress with `!` outside test arrange/assert.
- **Central package management:** all package versions live in [`Directory.Packages.props`](./Directory.Packages.props). Reference packages with `<PackageReference Include="..." />` and **no `Version` attribute**.
- **Shared MSBuild properties:** [`src/Directory.Build.props`](./src/Directory.Build.props) (package metadata, XML docs, Source Link, `Bielu.StaticCode.Analyzers`, coverage settings). **Version** is centralized in [`version.props`](./version.props) and written by the release tooling — never edit it or hardcode versions in csproj files.
- **Storage providers** implement `IMessageStore` / `IStoreTransaction` and ship their own `*ServiceCollectionExtensions` / `*StorageExtensions` hooking into the builder. A new backend is a new `Bielu.PersistentQueues.Storage.<Name>` project, not a change to the core.
- **Hot paths are allocation-sensitive:** messages use `ReadOnlyMemory<T>` / `ReadOnlySequence<byte>` rather than strings and arrays. Keep it that way, and measure with the benchmarks before claiming a performance change.
- **Formatting:** governed by [`.editorconfig`](./.editorconfig) (4-space indent for C#, final newline). File-scoped namespaces.
- **Style idioms** (follow the surrounding code): public APIs get XML doc comments; validate arguments with `ArgumentNullException.ThrowIfNull(...)`; configuration uses fluent, chainable extension methods.

## Engineering principles

- **SOLID** — small focused interfaces (`IMessageStore`, `IStoreTransaction`), dependencies registered and consumed through DI, behavior extended through new providers/extensions rather than edits to core queue logic.
- **DRY** — reuse existing extensions and helpers; shared MSBuild/package config already lives in `Directory.*.props` and `version.props`.
- **KISS** — prefer the simplest solution that fits the existing style; no speculative generality.
- **Proper abstraction** — mark implementation details `internal`; don't widen the public surface without need, since a published NuGet version is immutable.

## Code coverage

Target **70%+ line coverage** for code you add or change. [`coverlet.runsettings`](./coverlet.runsettings), applied to every test project via `src/Directory.Build.props`, turns the `coverlet.collector` on for every `dotnet test` run and measures only the `Bielu.*` library assemblies (tests, benchmarks, examples and generated sources are excluded). Cobertura reports land under each test project's `TestResults/`. Every test project must reference `coverlet.collector`.

## Build, test, format

```bash
# Restore + build the whole solution
dotnet build src/Bielu.PersistentQueues.slnx

# Run all tests (collects coverage too)
dotnet test src/Bielu.PersistentQueues.slnx

# Verify formatting / apply it
dotnet format --verify-no-changes src/Bielu.PersistentQueues.slnx
dotnet format src/Bielu.PersistentQueues.slnx
```

## Testing conventions

- **xUnit** + **Shouldly** assertions.
- Shared setup lives in `TestBase.cs`; network tests are under `Net/`, storage tests under `Storage/`.
- Name tests `Method_Scenario_ExpectedResult` (e.g. `MoveToDeadLetter_DlqQueueCreatedAutomatically`), matching the existing suite.
- Add or update tests for any behavior change, covering both the happy path and error/argument-validation paths.

## Versioning and releases

Releases are driven by [changesets](https://github.com/changesets/changesets) (see [`.changeset/README.md`](./.changeset/README.md)). Every PR that changes a package adds a changeset bumping `bielu-persistentqueues` (`npx changeset`); chore-only PRs use `npx changeset add --empty`. CI enforces this (`changeset-check`), except on Dependabot and Version Packages PRs. Merging to `main` publishes a beta when shipped code changed; merging the Version Packages PR publishes the stable release via nuget.org trusted publishing.

## Before opening a PR

1. `dotnet build` succeeds with no new warnings.
2. `dotnet test` passes.
3. `dotnet format --verify-no-changes src/Bielu.PersistentQueues.slnx` reports nothing new.
4. A changeset is included.
5. `readme.md` is updated when behavior or public APIs change.
6. Target the `main` branch.
