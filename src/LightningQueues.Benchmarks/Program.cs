using BenchmarkDotNet.Running;
using LightningQueues.Benchmarks;

var switcher = new BenchmarkSwitcher([
    typeof(SendAndReceive),
    typeof(LmdbStorageBenchmark),
    typeof(StorageProviderBenchmark)
]);

switcher.Run(args, new CustomConfig());