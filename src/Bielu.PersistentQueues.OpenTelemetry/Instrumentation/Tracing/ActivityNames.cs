namespace Bielu.PersistentQueues.OpenTelemetry.Instrumentation.Tracing;

internal static class ActivityNames
{
    public const string SourceName = "BieluPersistentQueues";

    public const string CreateQueue = "CreateQueue";
    public const string Start = "Start";
    public const string Receive = "Receive";
    public const string ReceiveBatch = "ReceiveBatch";
    public const string ProcessMessage = "ProcessMessage";
    public const string ProcessBatch = "ProcessBatch";
    public const string ReceiveLater = "ReceiveLater";
    public const string MoveToQueue = "MoveToQueue";
    public const string Send = "Send";
    public const string SendBatch = "SendBatch";
    public const string Enqueue = "Enqueue";
}
