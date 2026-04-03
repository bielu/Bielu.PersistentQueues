using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using Bielu.PersistentQueues;
using Bielu.PersistentQueues.Partitioning;

namespace Bielu.PersistentQueues.Examples.Strategies;

/// <summary>
/// Routes messages to partitions based on a pre-defined list of IDs loaded from a CSV string.
/// </summary>
/// <remarks>
/// Each ID in the list is assigned to a partition proportional to its position in the list:
/// IDs near the start map to lower partition numbers and IDs near the end map to higher numbers.
/// This means IDs that are adjacent in the list land in the same partition, which is useful when
/// the list order carries domain significance (e.g. customer registration order, account ranges).
///
/// Messages whose partition key is not found in the list fall back to FNV-1a hash-based routing so
/// that unknown IDs are still distributed evenly without throwing.
///
/// Typical use-case: you have a finite, known set of entity IDs (customer IDs, account numbers,
/// tenant slugs, etc.) and you want a deterministic, stable partition assignment per entity so that
/// all messages for the same entity always land in the same partition, preserving order.
/// </remarks>
public sealed class CsvListPartitionStrategy : IPartitionStrategy
{
    private readonly IReadOnlyDictionary<string, int> _idToIndex;
    private readonly int _totalIds;

    /// <summary>
    /// Initialises the strategy from a comma-separated string of IDs.
    /// </summary>
    /// <param name="csv">
    /// Comma-separated IDs, e.g. <c>"CUST-001,CUST-002,CUST-003"</c>.
    /// Leading/trailing whitespace around each entry is trimmed.
    /// </param>
    public CsvListPartitionStrategy(string csv)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(csv);

        var ids = csv.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        _totalIds = ids.Length;

        var dict = new Dictionary<string, int>(ids.Length, StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < ids.Length; i++)
            dict[ids[i]] = i;

        _idToIndex = dict;
    }

    /// <summary>
    /// Initialises the strategy from an in-memory list of IDs.
    /// </summary>
    public CsvListPartitionStrategy(IReadOnlyList<string> ids)
        : this(string.Join(',', ids)) { }

    /// <inheritdoc />
    public int GetPartition(Message message, int partitionCount)
    {
        if (partitionCount <= 0)
            throw new ArgumentOutOfRangeException(nameof(partitionCount), "Partition count must be greater than zero.");

        if (partitionCount == 1)
            return 0;

        if (!message.PartitionKey.IsEmpty)
        {
            var key = message.PartitionKey.ToString();

            if (_idToIndex.TryGetValue(key, out int index))
            {
                // Scale the ID's ordinal position in the list to a partition bucket.
                // IDs that appear consecutively in the list land in the same partition.
                // Example with 10 000 IDs and 2 000 partitions: every 5 consecutive IDs
                // share a partition, so the first 5 IDs go to partition 0, the next 5
                // to partition 1, and so on.
                return (int)((long)index * partitionCount / _totalIds);
            }

            // ID not in the list: fall back to a stable FNV-1a hash so the message
            // still gets a deterministic partition assignment.
            return (int)(ComputeFnv1a(message.PartitionKey.Span) % (uint)partitionCount);
        }

        // No partition key at all: distribute by message ID hash.
        return (int)((uint)message.Id.MessageIdentifier.GetHashCode() % (uint)partitionCount);
    }

    // FNV-1a hash – same algorithm used by HashPartitionStrategy for consistency.
    private static uint ComputeFnv1a(ReadOnlySpan<char> key)
    {
        const uint fnvOffsetBasis = 2166136261;
        const uint fnvPrime = 16777619;

        var bytes = MemoryMarshal.AsBytes(key);
        uint hash = fnvOffsetBasis;
        for (int i = 0; i < bytes.Length; i++)
        {
            hash ^= bytes[i];
            hash *= fnvPrime;
        }

        return hash;
    }
}
