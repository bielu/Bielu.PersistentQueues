using BenchmarkDotNet.Running;
using Bielu.PersistentQueues.Benchmarks;

var switcher = new BenchmarkSwitcher([
    typeof(SendAndReceive),
    typeof(LmdbStorageBenchmark),
    typeof(StorageProviderBenchmark),
    typeof(SlowStorageBenchmark)
]);

switcher.Run(args, new CustomConfig());