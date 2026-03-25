using Microsoft.Extensions.DependencyInjection;

namespace Bielu.PersistentQueues.OpenTelemetry;

public static class QueueExtensions
{
    public const string MetricName = "BieluPersistentQueues";
    public const string ActivityName = "BieluPersistentQueues";
    public static ServiceCollection AddBieluPersistentQueueInstrumentation(this ServiceCollection serviceCollection)
    {
        serviceCollection.Decorate<IQueue, PersistentQueueOtelDecorator>();
        return serviceCollection;
    }
}