namespace Bielu.PersistentQueues.Storage;

/// <summary>
/// Represents storage usage information for a message store.
/// </summary>
/// <param name="UsedBytes">The number of bytes currently used by the storage.</param>
/// <param name="TotalBytes">The total number of bytes allocated for the storage (map size).</param>
public readonly record struct StorageUsageInfo(long UsedBytes, long TotalBytes)
{
    /// <summary>
    /// Gets the storage usage as a percentage (0.0 to 100.0).
    /// Returns 0.0 if the total allocated size is zero.
    /// </summary>
    public double UsagePercentage => TotalBytes > 0
        ? (double)UsedBytes / TotalBytes * 100.0
        : 0.0;
}
