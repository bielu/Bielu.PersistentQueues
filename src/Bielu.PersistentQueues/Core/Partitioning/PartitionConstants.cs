namespace Bielu.PersistentQueues.Partitioning;

/// <summary>
/// Constants and helpers for partition queue naming conventions.
/// </summary>
public static class PartitionConstants
{
    /// <summary>
    /// The separator between a queue name and its partition index.
    /// </summary>
    public const string PartitionSeparator = ":partition-";

    /// <summary>
    /// Formats a partition queue name from a base queue name and partition index.
    /// </summary>
    /// <param name="queueName">The base queue name.</param>
    /// <param name="partitionIndex">The zero-based partition index.</param>
    /// <returns>The full partition queue name (e.g., "orders:partition-0").</returns>
    public static string FormatPartitionQueueName(string queueName, int partitionIndex)
    {
        return $"{queueName}{PartitionSeparator}{partitionIndex}";
    }

    /// <summary>
    /// Tries to parse a partition queue name back into its base name and partition index.
    /// </summary>
    /// <param name="partitionQueueName">The partition queue name to parse.</param>
    /// <param name="queueName">The extracted base queue name.</param>
    /// <param name="partitionIndex">The extracted partition index.</param>
    /// <returns>True if parsing succeeded; false otherwise.</returns>
    public static bool TryParsePartitionQueueName(string partitionQueueName, out string queueName, out int partitionIndex)
    {
        var separatorIndex = partitionQueueName.LastIndexOf(PartitionSeparator);
        if (separatorIndex >= 0)
        {
            queueName = partitionQueueName[..separatorIndex];
            var indexStr = partitionQueueName[(separatorIndex + PartitionSeparator.Length)..];
            if (int.TryParse(indexStr, out partitionIndex))
                return true;
        }

        queueName = partitionQueueName;
        partitionIndex = -1;
        return false;
    }
}
