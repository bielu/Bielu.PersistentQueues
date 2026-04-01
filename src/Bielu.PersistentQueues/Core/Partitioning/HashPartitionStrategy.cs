using System;
using System.Runtime.InteropServices;

namespace Bielu.PersistentQueues.Partitioning;

/// <summary>
/// Assigns messages to partitions based on a hash of their partition key.
/// </summary>
/// <remarks>
/// Messages with the same partition key are always routed to the same partition,
/// guaranteeing ordering for related messages. Messages without a partition key
/// are assigned based on a hash of their message identifier, providing even
/// distribution across partitions.
/// </remarks>
public class HashPartitionStrategy : IPartitionStrategy
{
    /// <inheritdoc />
    public int GetPartition(Message message, int partitionCount)
    {
        if (partitionCount <= 0)
            throw new ArgumentOutOfRangeException(nameof(partitionCount), "Partition count must be greater than zero.");

        if (partitionCount == 1)
            return 0;

        uint hash;
        if (!message.PartitionKey.IsEmpty)
        {
            hash = ComputeHash(message.PartitionKey.Span);
        }
        else
        {
            // Fall back to message identifier for even distribution
            hash = (uint)message.Id.MessageIdentifier.GetHashCode();
        }

        return (int)(hash % (uint)partitionCount);
    }

    /// <summary>
    /// Computes a stable FNV-1a hash from a character span.
    /// </summary>
    private static uint ComputeHash(ReadOnlySpan<char> key)
    {
        // FNV-1a hash — fast, simple, and produces good distribution
        const uint fnvOffsetBasis = 2166136261;
        const uint fnvPrime = 16777619;

        var byteSpan = MemoryMarshal.AsBytes(key);
        uint hash = fnvOffsetBasis;
        for (int i = 0; i < byteSpan.Length; i++)
        {
            hash ^= byteSpan[i];
            hash *= fnvPrime;
        }

        return hash;
    }
}
