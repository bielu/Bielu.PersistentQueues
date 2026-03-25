using System.Diagnostics;
using OpenTelemetry.Trace;

namespace Bielu.PersistentQueues.OpenTelemetry.Instrumentation.Tracing;

internal sealed class QueueActivitySource
{
    private readonly ActivitySource _activitySource;

    public QueueActivitySource()
    {
        _activitySource = new ActivitySource(ActivityNames.SourceName, "1.0.0");
    }

    public Activity? StartActivity(string name, ActivityKind kind)
    {
        return _activitySource.StartActivity(name, kind);
    }

    public void RecordException(Activity? activity, Exception exception)
    {
        if (activity == null) return;

        activity.SetStatus(ActivityStatusCode.Error, exception.Message);
        activity.AddException(exception);
    }

    public static void SetQueueTags(Activity? activity, string queueName)
    {
        activity?.SetTag("queue.name", queueName);
    }

    public static void SetMessageTags(Activity? activity, Guid messageId, string? queueName = null, string? destination = null)
    {
        activity?.SetTag("message.id", messageId);
        if (queueName != null)
        {
            activity?.SetTag("message.queue", queueName);
        }
        if (destination != null)
        {
            activity?.SetTag("destination", destination);
        }
    }

    public static void SetBatchTags(Activity? activity, int batchSize, string? queueName = null)
    {
        activity?.SetTag("batch.size", batchSize);
        if (queueName != null)
        {
            activity?.SetTag("queue.name", queueName);
        }
    }

    public static void SetDelayTags(Activity? activity, TimeSpan timeSpan)
    {
        activity?.SetTag("delay.timespan", timeSpan.ToString());
        activity?.SetTag("delay.seconds", timeSpan.TotalSeconds);
    }

    public static void SetScheduledTimeTags(Activity? activity, DateTimeOffset time)
    {
        activity?.SetTag("scheduled.time", time.ToString("O"));
    }

    public static void SetPollIntervalTag(Activity? activity, int pollIntervalInMilliseconds)
    {
        activity?.SetTag("poll.interval", pollIntervalInMilliseconds);
    }
}
