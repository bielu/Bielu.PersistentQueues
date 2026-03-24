using BenchmarkDotNet.Running;
using Bielu.PersistentQueues.Benchmarks;

var switcher = new BenchmarkSwitcher([
    typeof(SendAndReceive),
    typeof(LmdbStorageBenchmark)
]);

switcher.Run(args, new CustomConfig());