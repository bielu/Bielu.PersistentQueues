namespace Bielu.PersistentQueues.Storage.ZoneTree;

/// <summary>
/// Configuration options for ZoneTree storage.
/// </summary>
public class ZoneTreeStorageOptions
{
    /// <summary>
    /// Gets or sets the maximum number of items in the mutable segment before it is
    /// moved forward (flushed to a read-only segment). Default is 1,000,000.
    /// Lower values reduce memory usage but increase merge frequency.
    /// </summary>
    public int MutableSegmentMaxItemCount { get; set; } = 1_000_000;

    /// <summary>
    /// Gets or sets the maximum number of items in a single disk segment.
    /// Default is 20,000,000.
    /// </summary>
    public int DiskSegmentMaxItemCount { get; set; } = 20_000_000;

    /// <summary>
    /// Gets or sets whether to enable background merge operations via a maintainer.
    /// Default is true. When enabled, a maintainer is created for each queue to handle
    /// compaction and cache management automatically.
    /// </summary>
    public bool EnableMaintainer { get; set; } = true;

    /// <summary>
    /// Creates default options suitable for most workloads.
    /// <summary>
/// Create a ZoneTreeStorageOptions configured with the class defaults.
/// </summary>
/// <returns>A new <see cref="ZoneTreeStorageOptions"/> initialized with the default property values.</returns>
    public static ZoneTreeStorageOptions Default() => new();

    /// <summary>
    /// Creates options optimized for high write throughput with larger in-memory buffers.
    /// <summary>
    /// Create a ZoneTreeStorageOptions instance tuned for high-throughput workloads.
    /// </summary>
    /// <returns>A new ZoneTreeStorageOptions with MutableSegmentMaxItemCount = 5,000,000 and DiskSegmentMaxItemCount = 50,000,000; EnableMaintainer remains at its default value.</returns>
    public static ZoneTreeStorageOptions HighThroughput() => new()
    {
        MutableSegmentMaxItemCount = 5_000_000,
        DiskSegmentMaxItemCount = 50_000_000
    };

    /// <summary>
    /// Creates options optimized for low memory usage with smaller buffers.
    /// <summary>
    /// Creates a ZoneTreeStorageOptions instance preconfigured for low-memory environments.
    /// </summary>
    /// <returns>A new ZoneTreeStorageOptions with MutableSegmentMaxItemCount = 100000 and DiskSegmentMaxItemCount = 5000000.</returns>
    public static ZoneTreeStorageOptions LowMemory() => new()
    {
        MutableSegmentMaxItemCount = 100_000,
        DiskSegmentMaxItemCount = 5_000_000
    };
}
