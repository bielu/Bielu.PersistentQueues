namespace Bielu.PersistentQueues.Storage;

/// <summary>
/// Represents storage usage information for a message store.
/// </summary>
/// <param name="UsedBytes">The number of bytes currently used by the storage (high-water mark of the data file).</param>
/// <param name="TotalBytes">The total number of bytes allocated for the storage (map size).</param>
/// <param name="FreeBytes">The number of bytes in the free list, available for reuse by the storage.</param>
public readonly record struct StorageUsageInfo(long UsedBytes, long TotalBytes, long FreeBytes = 0)
{
    /// <summary>
    /// Gets the net used bytes (UsedBytes - FreeBytes).
    /// </summary>
    public long NetUsedBytes => UsedBytes - FreeBytes;

    /// <summary>
    /// Gets the storage usage as a percentage (0.0 to 100.0) based on the high-water mark.
    /// Returns 0.0 if the total allocated size is zero.
    /// </summary>
    public double UsagePercentage => TotalBytes > 0
        ? (double)UsedBytes / TotalBytes * 100.0
        : 0.0;

    /// <summary>
    /// Gets the net storage usage as a percentage (0.0 to 100.0) excluding free pages.
    /// Returns 0.0 if the total allocated size is zero.
    /// </summary>
    public double NetUsagePercentage => TotalBytes > 0
        ? (double)NetUsedBytes / TotalBytes * 100.0
        : 0.0;
}
