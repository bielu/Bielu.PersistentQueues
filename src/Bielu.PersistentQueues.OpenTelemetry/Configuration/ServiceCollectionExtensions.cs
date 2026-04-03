using Bielu.PersistentQueues.OpenTelemetry.Instrumentation;
using Bielu.PersistentQueues.OpenTelemetry.Instrumentation.Metrics;
using Bielu.PersistentQueues.OpenTelemetry.Instrumentation.Tracing;
using Bielu.PersistentQueues.Partitioning;
using Microsoft.Extensions.DependencyInjection;
using System.Linq;

namespace Bielu.PersistentQueues.OpenTelemetry.Configuration;

public static class ServiceCollectionExtensions
{
    public const string MetricName = "BieluPersistentQueues";
    public const string ActivityName = "BieluPersistentQueues";

    public static IServiceCollection AddBieluPersistentQueueInstrumentation(this IServiceCollection serviceCollection)
    {
        serviceCollection.AddSingleton<QueueMetrics>();
        serviceCollection.AddSingleton<QueueActivitySource>();

        serviceCollection.Decorate<IQueue, PersistentQueueOtelDecorator>();

        // Only decorate IPartitionedQueue if it has been registered in the container
        if (serviceCollection.Any(sd => sd.ServiceType == typeof(IPartitionedQueue)))
        {
            serviceCollection.Decorate<IPartitionedQueue, PartitionedQueueOtelDecorator>();
        }

        return serviceCollection;
    }
}
