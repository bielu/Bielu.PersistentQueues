using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;

namespace Bielu.PersistentQueues.OpenTelemetry.Configuration;

public static class OpenTelemetryExtensions
{
    public static MeterProviderBuilder AddBieluPersistentQueuesInstrumentation(this MeterProviderBuilder builder)
    {
        return builder.AddMeter(ServiceCollectionExtensions.MetricName);
    }

    public static TracerProviderBuilder AddBieluPersistentQueuesInstrumentation(this TracerProviderBuilder builder)
    {
        return builder.AddSource(ServiceCollectionExtensions.ActivityName);
    }
}
