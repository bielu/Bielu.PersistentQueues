# Bielu.PersistentQueues Benchmarks

Performance benchmarking suite for Bielu.PersistentQueues using BenchmarkDotNet.

## Quick Start

### Run All Benchmarks
```bash
dotnet run -c Release
```

### Run Specific Benchmark Suite
```bash
# Regression benchmarks (fast, used in CI)
dotnet run -c Release -- --filter "*RegressionBenchmark*"

# Storage provider comparison
dotnet run -c Release -- --filter "*StorageProviderBenchmark*"

# LMDB-specific benchmarks
dotnet run -c Release -- --filter "*LmdbStorageBenchmark*"

# End-to-end send/receive
dotnet run -c Release -- --filter "*SendAndReceive*"
```

### Run Interactive Mode
```bash
dotnet run -c Release
# Then select benchmark from menu
```

## Benchmark Suites

### RegressionBenchmark
**Purpose**: Fast performance regression detection for CI/CD pipeline
**Runtime**: ~5 minutes
**Scale**: 100-1,000 messages, 64-512 bytes

**Operations tested:**
- `Enqueue` - Message insertion
- `ReceiveAndAcknowledge` - Single message processing
- `BatchReceiveAndAcknowledge` - Batch processing (10 msg/batch)
- `ReceiveLater` - Retry/delay pattern
- `MoveTo` - Queue transfer
- `BatchMixedOperations` - Mixed batch operations

**When to run**: Before submitting PRs with performance-sensitive changes

### StorageProviderBenchmark
**Purpose**: Compare different storage backends (LMDB, ZoneTree, FASTER)
**Runtime**: ~30 minutes
**Scale**: 100-1,000,000 messages

**When to run**: Evaluating storage alternatives

### LmdbStorageBenchmark
**Purpose**: Compare LMDB configuration options
**Runtime**: ~10 minutes
**Scale**: 10-100 messages

**When to run**: LMDB optimization work

### SendAndReceive
**Purpose**: End-to-end message flow performance
**Runtime**: ~15 minutes
**Modes**: Default, AppendData, NoSync, MaxThroughput

**When to run**: Testing full pipeline performance

## CI/CD Integration

Benchmarks run automatically on pull requests:

1. **RegressionBenchmark** runs on every PR
2. Results compared against baseline from `main` branch
3. PR fails if performance regression > 10% detected
4. Results posted as PR comment

See [BENCHMARK_STRATEGY.md](../../BENCHMARK_STRATEGY.md) for details.

## Analyzing Results

### Key Metrics

| Metric | Description | Target |
|--------|-------------|--------|
| Mean | Average execution time | Lower is better |
| Error | Statistical error margin | Lower is better |
| StdDev | Standard deviation | Lower is better (consistency) |
| Allocated | Memory allocated per operation | Lower is better |
| Gen0/1/2 | GC collection counts | Lower is better |

### Example Output
```
| Method                    | MessageCount | Mean      | Error    | Allocated |
|-------------------------- |------------- |----------:|---------:|----------:|
| Enqueue                   | 100          | 1.234 ms  | 0.012 ms | 15.2 KB   |
| ReceiveAndAcknowledge     | 100          | 2.345 ms  | 0.023 ms | 22.4 KB   |
| BatchReceiveAndAcknowledge| 100          | 1.567 ms  | 0.015 ms | 18.1 KB   |
```

### Comparing Results

```bash
# Install comparison tool
dotnet tool install -g BenchmarkDotNet.ResultsComparer

# Run baseline
dotnet run -c Release -- --filter "*RegressionBenchmark*" --exporters json
cp -r BenchmarkDotNet.Artifacts/results baseline/

# Make changes and run again
git checkout feature-branch
dotnet run -c Release -- --filter "*RegressionBenchmark*" --exporters json

# Compare
dotnet-results-comparer \
  --base baseline/results.json \
  --diff BenchmarkDotNet.Artifacts/results/results.json \
  --threshold 5%
```

## Best Practices

### Before Running Benchmarks

1. Close unnecessary applications
2. Disable CPU frequency scaling (for consistent results)
3. Use Release configuration (`-c Release`)
4. Run on consistent hardware (especially for comparisons)

### Interpreting Changes

- **< 5%**: Within noise margin
- **5-10%**: Potentially significant, investigate
- **> 10%**: Significant change, document reason

### Common Pitfalls

❌ **Don't**:
- Run benchmarks in Debug mode
- Compare results from different machines
- Run while system is under load
- Ignore memory allocation increases

✅ **Do**:
- Run multiple times to verify consistency
- Check allocation patterns, not just speed
- Profile with dotMemory/dotTrace for deep analysis
- Document expected performance trade-offs

## Advanced Usage

### Custom Configuration

Edit `CustomConfig.cs` to modify:
- Job configuration
- Exporters (JSON, HTML, CSV)
- Diagnosers (Memory, Threading, etc.)
- Validators

### Memory Profiling

```bash
# With dotMemory diagnoser
dotnet run -c Release -- \
  --filter "*RegressionBenchmark*" \
  --memory \
  --diagnosers dotMemory
```

### Export to Multiple Formats

```bash
dotnet run -c Release -- \
  --filter "*RegressionBenchmark*" \
  --exporters json,html,csv
```

### Filter by Method

```bash
# Run only Enqueue benchmarks
dotnet run -c Release -- --filter "*Enqueue*"

# Run only batch operations
dotnet run -c Release -- --filter "*Batch*"
```

## Troubleshooting

### "BenchmarkDotNet.Toolchains.CsProjCoreToolchain required"
Ensure you're running in Release mode: `dotnet run -c Release`

### Out of Memory
Reduce message count parameters or increase LMDB MapSize in benchmark setup.

### Results Unstable/High Variance
- Close background applications
- Increase iteration count
- Run on dedicated machine
- Check for thermal throttling

### "No baseline for comparison"
Run the baseline workflow on main branch first, or create local baseline.

## Contributing

When adding new benchmarks:

1. Add to `RegressionBenchmark.cs` only if it should run on every PR
2. Keep regression benchmarks fast (< 5 min total)
3. Use `[Params]` for testing different configurations
4. Document the benchmark purpose in XML comments
5. Update this README

## Resources

- [BenchmarkDotNet Documentation](https://benchmarkdotnet.org/)
- [Project Benchmark Strategy](../../BENCHMARK_STRATEGY.md)
- [Performance Best Practices](https://benchmarkdotnet.org/articles/guides/good-practices.html)
