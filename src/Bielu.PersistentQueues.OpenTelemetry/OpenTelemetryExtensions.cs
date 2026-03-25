using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;

namespace Bielu.PersistentQueues.OpenTelemetry;

public static class OpenTelemetryExtensions
{
    public static MeterProviderBuilder AddBieluPersistentQueuesInstrumentation(this MeterProviderBuilder builder)
    {
        return builder.AddMeter(QueueExtensions.MetricName);
    }

    public static TracerProviderBuilder AddBieluPersistentQueuesInstrumentation(this TracerProviderBuilder builder)
    {
        return builder.AddSource(QueueExtensions.ActivityName);
    }
}