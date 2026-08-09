# Contributing to Bielu.PersistentQueues

Thank you for your interest in contributing to Bielu.PersistentQueues! We welcome contributions from the community and are grateful for your help in making this project better.

## Table of Contents

- [Getting Started](#getting-started)
- [Prerequisites](#prerequisites)
- [Development Setup](#development-setup)
- [Building the Project](#building-the-project)
- [Running Tests](#running-tests)
- [Pull Request Process](#pull-request-process)
- [Release Process](#release-process)

## Getting Started

1. Fork the repository on GitHub
2. Clone your fork locally:
   ```bash
   git clone https://github.com/YOUR_USERNAME/Bielu.PersistentQueues.git
   cd Bielu.PersistentQueues
   ```
3. Add the upstream repository as a remote:
   ```bash
   git remote add upstream https://github.com/bielu/Bielu.PersistentQueues.git
   ```
4. Create a branch for your changes:
   ```bash
   git checkout -b feature/your-feature-name
   ```

## Prerequisites

Before you begin, ensure you have the following installed:

- **.NET SDK 10.0** or later
- **Node.js 22.x** or later (for `npx changeset` — see [Release Process](#release-process))
- **Git**
- An IDE such as Visual Studio, Visual Studio Code, or JetBrains Rider

## Development Setup

### 1. Restore NuGet Packages

```bash
dotnet restore
```

### 2. Build the Solution

```bash
dotnet build
```

## Building the Project

### Local Build

Build the entire solution:

```bash
dotnet build src/Bielu.PersistentQueues.slnx
```

### Create NuGet Packages Locally

```bash
dotnet pack src/Bielu.PersistentQueues/Bielu.PersistentQueues.csproj --configuration Release --output ./build
```

## Running Tests

### Run All Tests

```bash
dotnet test
```

### Run Tests with Code Coverage

```bash
dotnet test --collect:"XPlat Code Coverage"
```

## Pull Request Process

1. **Ensure your code builds** without errors:
   ```bash
   dotnet build --configuration Debug
   ```

2. **Run the tests** and ensure they pass:
   ```bash
   dotnet test
   ```

3. **Update documentation** if you've changed public APIs

4. **Add a changeset** describing your change (see [Release Process](#release-process) below):
   ```bash
   npx changeset
   ```
   For CI-only, development-tooling-dependency, or documentation-only changes, record an empty
   changeset instead:
   ```bash
   npx changeset add --empty
   ```
   A NuGet package dependency bump (or any dependency change that affects runtime behavior) ships in
   the published packages, so it needs a normal changeset — not an empty one.

5. **Commit your changes** with a clear, descriptive commit message

6. **Push to your fork** and create a pull request against the `main` branch

7. **Wait for CI checks** to pass — a PR without a changeset fails the `changeset-check` job

## Release Process

Releases are managed through GitHub Actions using [changesets](https://github.com/changesets/changesets)
and follow semantic versioning. Maintainers do not edit `version.props` or `CHANGELOG.md` by hand —
see [`.changeset/README.md`](.changeset/README.md) for how to author a changeset.

### Version File

The shared version for all four NuGet packages is managed centrally in the `version.props` file in
the repository root, written automatically by the changeset `version` step:

```xml
<Project>
    <PropertyGroup>
        <VersionPrefix>1.0.0</VersionPrefix>
        <VersionSuffix></VersionSuffix>
    </PropertyGroup>
</Project>
```

- **VersionPrefix**: The base version number (MAJOR.MINOR.PATCH)
- **VersionSuffix**: Optional suffix for pre-release versions (e.g., `beta.<timestamp>`)

To change the version, add a changeset describing your change (see above); the accumulated
changesets on `main` determine the next version bump. Since all four packages
(`Bielu.PersistentQueues`, `Bielu.PersistentQueues.Storage.LMDB`,
`Bielu.PersistentQueues.Storage.ZoneTree`, `Bielu.PersistentQueues.OpenTelemetry`) share this one
version, a changeset bumps the single placeholder package `bielu-persistentqueues`
(`build/changeset/nuget-suite`) rather than an individual `.csproj`.

### CI Pipeline

The CI workflow ([.github/workflows/buildAndPublishPackage.yml](./.github/workflows/buildAndPublishPackage.yml)) runs on:
- Every push to `main`
- Every pull request

It performs:
- Building the solution
- Running unit tests
- NuGet package creation
- Requiring a changeset on pull requests

### Package Publishing

- **Pull requests**: Packages are built with a `-pr` suffix (not published)
- **Pushes to main while changesets are pending**: Packages are built as
  `<pending-version>-beta.<UTC timestamp>` prereleases and published to NuGet, and surfaced as a
  GitHub prerelease listing the pending changes
- **Merging the "Version Packages" PR**: Packages are built at the version committed to
  `version.props` (no suffix) and published to NuGet; a matching `vX.Y.Z` GitHub Release is created
  from that version's [`CHANGELOG.md`](./CHANGELOG.md) section

### Creating a Release

Releases are cut by merging the automated **"Version Packages"** PR that the changesets action keeps
open on `main` while unreleased changesets exist:

1. PRs merge to `main`, each carrying a changeset. The bot PR accumulates them into a version bump
   and changelog entries. Meanwhile, each push to `main` while changesets remain pending also
   publishes an interim beta prerelease, numbered from the version the pending changesets are
   heading for — not the version already released, so it never sorts below the last stable release.
2. A maintainer reviews and merges the Version Packages PR.
3. The workflow then publishes the committed version to NuGet.org and creates the matching
   `vX.Y.Z` GitHub Release.

To see which bump is currently pending without opening the PR:

```bash
npm run changeset:status
```

### Version Guidelines

- Use **semantic versioning** (MAJOR.MINOR.PATCH)
- Increment MAJOR for breaking changes
- Increment MINOR for new features (backward compatible)
- Increment PATCH for bug fixes

---

Thank you for contributing to Bielu.PersistentQueues! 🚀
