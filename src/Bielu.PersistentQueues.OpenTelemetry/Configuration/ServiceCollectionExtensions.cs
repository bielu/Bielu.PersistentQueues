using Bielu.PersistentQueues.OpenTelemetry.Instrumentation;
using Bielu.PersistentQueues.OpenTelemetry.Instrumentation.Metrics;
using Bielu.PersistentQueues.OpenTelemetry.Instrumentation.Tracing;
using Bielu.PersistentQueues.Partitioning;
using Microsoft.Extensions.DependencyInjection;

namespace Bielu.PersistentQueues.OpenTelemetry.Configuration;

public static class ServiceCollectionExtensions
{
    public const string MetricName = "BieluPersistentQueues";
    public const string ActivityName = "BieluPersistentQueues";

    public static IServiceCollection AddBieluPersistentQueueInstrumentation(this IServiceCollection serviceCollection)
    {
        serviceCollection.Decorate<IQueue, PersistentQueueOtelDecorator>();
        serviceCollection.AddSingleton<QueueMetrics>();
        serviceCollection.AddSingleton<QueueActivitySource>();

        // If IPartitionedQueue is registered, decorate it too
        serviceCollection.Decorate<IPartitionedQueue, PartitionedQueueOtelDecorator>();

        return serviceCollection;
    }
}
