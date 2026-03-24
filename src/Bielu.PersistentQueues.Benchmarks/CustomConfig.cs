using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Loggers;

namespace Bielu.PersistentQueues.Benchmarks;

public class CustomConfig : ManualConfig
{
    public CustomConfig()
    {
        AddLogger(ConsoleLogger.Default);
        AddColumnProvider(DefaultColumnProviders.Instance);
    }
}