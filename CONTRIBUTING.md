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
dotnet build LightningQueues.sln
```

### Create NuGet Packages Locally

```bash
dotnet pack src/LightningQueues --configuration Release --output ./build
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

4. **Commit your changes** with a clear, descriptive commit message

5. **Push to your fork** and create a pull request against the `main` branch

6. **Wait for CI checks** to pass

## Release Process

Releases are managed through GitHub Actions and follow semantic versioning.

### Version File

The project version is managed centrally in the `version.props` file in the repository root:

```xml
<Project>
    <PropertyGroup>
        <VersionPrefix>0.6.0</VersionPrefix>
        <VersionSuffix></VersionSuffix>
    </PropertyGroup>
</Project>
```

- **VersionPrefix**: The base version number (MAJOR.MINOR.PATCH)
- **VersionSuffix**: Optional suffix for pre-release versions (e.g., `beta`, `rc1`)

### CI Pipeline

The CI workflow runs on:
- Every push to `main`
- Every pull request

It performs:
- Building the solution
- Running unit tests
- NuGet package creation

### Package Publishing

- **Pull Requests**: Packages are built with a `-pr` suffix (not published)
- **Pushes to main**: Packages are built with a `-beta` suffix and published to NuGet
- **Releases**: Packages are built without suffix and published to NuGet

### Version Guidelines

- Use **semantic versioning** (MAJOR.MINOR.PATCH)
- Increment MAJOR for breaking changes
- Increment MINOR for new features (backward compatible)
- Increment PATCH for bug fixes

---

Thank you for contributing to Bielu.PersistentQueues! 🚀
