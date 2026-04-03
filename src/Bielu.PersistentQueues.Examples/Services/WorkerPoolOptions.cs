namespace Bielu.PersistentQueues.Examples.Services;

/// <summary>
/// Options that control how many partition workers are active.
/// Bound to the "WorkerPool" section of appsettings.json; changes are
/// picked up at runtime by <see cref="PartitionCoordinatorService"/> via
/// <c>IOptionsMonitor&lt;WorkerPoolOptions&gt;</c> — no restart required.
/// </summary>
internal sealed class WorkerPoolOptions
{
    public const string Section = "WorkerPool";

    /// <summary>Number of concurrent partition-consumer workers (min 1).</summary>
    public int WorkerCount { get; set; } = 30;
}
