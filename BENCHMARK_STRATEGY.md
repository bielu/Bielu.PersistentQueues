# Performance Benchmark Strategy

This document outlines the performance benchmarking strategy for Bielu.PersistentQueues, including both development benchmarks and CI/CD pipeline integration.

## Overview

The project uses [BenchmarkDotNet](https://benchmarkdotnet.org/) for performance testing with two distinct benchmark suites:

1. **Development Benchmarks** - Comprehensive performance analysis for local development
2. **Regression Benchmarks** - Lightweight CI/CD benchmarks for detecting performance regressions in PRs

## Benchmark Suites

### Development Benchmarks (Local Use)

Located in `src/Bielu.PersistentQueues.Benchmarks/`, these are comprehensive benchmarks for in-depth performance analysis:

#### `StorageProviderBenchmark.cs`
- **Purpose**: Compare storage backends (LMDB, ZoneTree, FASTER KV)
- **Operations**: Put, Get, Enumerate, Delete, Mixed workloads
- **Scale**: 100 to 1,000,000 messages
- **When to run**: Evaluating storage options or investigating storage performance issues

#### `LmdbStorageBenchmark.cs`
- **Purpose**: Compare LMDB configuration options
- **Tests**: Default vs GuidComparer, serialization paths, AppendData mode
- **Scale**: 10 to 100 messages
- **When to run**: LMDB-specific optimizations

#### `SendAndReceive.cs`
- **Purpose**: End-to-end message flow benchmarks
- **Modes**: Default, AppendData, NoSync, MaxThroughput
- **Scale**: 100 messages
- **When to run**: Testing full queue pipeline performance

#### `SlowStorageBenchmark.cs`
- **Purpose**: Test behavior under storage latency
- **When to run**: Validating queue behavior with slow/network storage

### Regression Benchmarks (CI/CD Pipeline)

#### `RegressionBenchmark.cs`
- **Purpose**: Fast regression detection for PRs
- **Operations**:
  - `Enqueue` - Message insertion performance
  - `ReceiveAndAcknowledge` - Single message processing
  - `BatchReceiveAndAcknowledge` - Batch processing (10 messages/batch)
  - `ReceiveLater` - Retry pattern performance
  - `MoveTo` - Queue transfer performance
  - `BatchMixedOperations` - Mixed operations (delay, move, acknowledge)
- **Scale**: 100 and 1,000 messages with 64 and 512 byte payloads
- **Target runtime**: < 5 minutes
- **Execution**: 5 iterations, 1 warmup (fast mode)

## CI/CD Integration

### Workflow: `benchmark-pr.yml`

**Triggers**: Pull requests to `main` or `develop` branches affecting:
- `src/Bielu.PersistentQueues/**`
- `src/Bielu.PersistentQueues.Storage.LMDB/**`
- `src/Bielu.PersistentQueues.Benchmarks/**`

**Process**:
1. Run `RegressionBenchmark` suite
2. Compare results with baseline from main branch
3. Post comparison comment on PR
4. **Fail PR** if regressions > 10% threshold detected
5. Upload results as artifacts (90-day retention)

**Outputs**:
- PR comment with performance comparison table
- Downloadable benchmark artifacts
- Pass/fail status for performance gate

### Workflow: `benchmark-baseline.yml`

**Triggers**:
- Push to `main` branch
- Manual dispatch

**Process**:
1. Run `RegressionBenchmark` suite on main branch
2. Store results as baseline artifact (365-day retention)
3. Optionally commit baseline to `.benchmarks/` directory

**Purpose**: Maintain up-to-date performance baseline for PR comparisons

## Running Benchmarks Locally

### Run All Development Benchmarks
```bash
cd src/Bielu.PersistentQueues.Benchmarks
dotnet run -c Release
```

### Run Specific Benchmark
```bash
dotnet run -c Release -- --filter "*RegressionBenchmark*"
dotnet run -c Release -- --filter "*StorageProviderBenchmark*"
```

### Run Regression Suite (CI-equivalent)
```bash
dotnet run -c Release -- \
  --filter "*RegressionBenchmark*" \
  --exporters json \
  --artifacts ./BenchmarkDotNet.Artifacts
```

### Compare Two Benchmark Runs
```bash
# Install comparer tool
dotnet tool install -g BenchmarkDotNet.ResultsComparer

# Run baseline
dotnet run -c Release -- --filter "*RegressionBenchmark*" --exporters fulljson
mv BenchmarkDotNet.Artifacts/results baseline-results

# Make changes, then run again
dotnet run -c Release -- --filter "*RegressionBenchmark*" --exporters fulljson

# Compare (ResultsComparer expects directories of *-report-full.json files)
dotnet-results-comparer \
  --base baseline-results/ \
  --diff BenchmarkDotNet.Artifacts/results/ \
  --threshold 5%
```

## Performance Guidelines

### What Triggers Performance Review

PRs should include benchmark results if they:
- Modify core message processing logic
- Change storage layer implementation
- Alter batch processing algorithms
- Add new features to hot paths
- Modify serialization/deserialization

### Acceptable Performance Changes

- **< 5%**: Noise/acceptable variation
- **5-10%**: Requires explanation in PR description
- **> 10%**: Triggers CI failure, requires justification and approval
- **Improvements**: Always welcome, document optimization approach

### When to Update Baseline

Baselines are automatically updated on merge to `main`. Manual baseline updates via workflow dispatch may be needed after:
- Infrastructure changes (new CI runners)
- .NET version upgrades
- Intentional performance trade-offs (e.g., correctness over speed)

## Interpreting Results

### Key Metrics

- **Mean**: Primary metric for comparison
- **Median**: More stable for variable workloads
- **Allocated**: Memory allocation pressure (lower is better)
- **Gen0/Gen1/Gen2**: GC collections (lower is better)

### Red Flags

- Increased allocations without functionality changes
- Higher GC pressure (especially Gen2)
- Superlinear scaling (O(n²) instead of O(n))
- Increased variance (Min-Max spread)

## Benchmark Maintenance

### Adding New Benchmarks

1. Add to `RegressionBenchmark.cs` if it should run on every PR
2. Create new file for specialized/long-running benchmarks
3. Update `Program.cs` to include new benchmark class
4. Keep regression benchmarks fast (< 5 min total)

### Removing Benchmarks

- Never remove from `RegressionBenchmark` without team discussion
- Document reason for removal in PR
- Archive old benchmark code for historical reference

## Best Practices

### For Contributors

- Run regression benchmarks locally before submitting performance-sensitive PRs
- Include benchmark results in PR description for significant changes
- Explain any expected performance trade-offs
- Use `[Benchmark(Baseline = true)]` when comparing implementations

### For Reviewers

- Check for unexpected allocation increases
- Verify scaling characteristics with different message counts
- Question regressions > 5% without clear explanation
- Ensure new features include regression benchmarks

## Troubleshooting

### CI Benchmark Failures

**"Timeout after 20 minutes"**
- RegressionBenchmark taking too long
- Reduce iteration count or message counts
- Check for infinite loops or deadlocks

**"No baseline for comparison"**
- First run on new branch
- Baseline workflow hasn't completed on main
- Run baseline workflow manually

**"Performance regression detected"**
- Review comparison output in PR comment
- Reproduce locally with same parameters
- Profile with dotMemory/dotTrace if needed
- Justify if intentional trade-off

### Local Benchmark Issues

**"Results unstable/high variance"**
- Close other applications
- Disable CPU frequency scaling
- Run multiple times and check consistency
- Use `[IterationCount]` attribute to increase samples

**"Out of memory"**
- Reduce `MessageCount` parameter
- Check for memory leaks in benchmark setup/cleanup
- Increase LMDB MapSize if needed

## Future Enhancements

Potential improvements to benchmark strategy:

- [ ] Historical trend tracking (performance over time)
- [ ] Cross-platform benchmarks (Windows/Linux/macOS)
- [ ] Benchmark result visualization dashboard
- [ ] Automated performance regression reports
- [ ] Memory leak detection integration
- [ ] Percentile-based SLO tracking (p50, p99)
- [ ] Continuous benchmarking (scheduled runs)

## References

- [BenchmarkDotNet Documentation](https://benchmarkdotnet.org/)
- [BenchmarkDotNet Best Practices](https://benchmarkdotnet.org/articles/guides/good-practices.html)
- [Performance Testing in .NET](https://learn.microsoft.com/en-us/dotnet/core/testing/performance-testing)
