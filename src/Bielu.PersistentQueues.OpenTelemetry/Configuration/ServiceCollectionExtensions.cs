using Bielu.PersistentQueues.OpenTelemetry.Instrumentation;
using Microsoft.Extensions.DependencyInjection;

namespace Bielu.PersistentQueues.OpenTelemetry.Configuration;

public static class ServiceCollectionExtensions
{
    public const string MetricName = "BieluPersistentQueues";
    public const string ActivityName = "BieluPersistentQueues";

    public static ServiceCollection AddBieluPersistentQueueInstrumentation(this IServiceCollection serviceCollection)
    {
        serviceCollection.Decorate<IQueue, PersistentQueueOtelDecorator>();
        return serviceCollection;
    }
}
