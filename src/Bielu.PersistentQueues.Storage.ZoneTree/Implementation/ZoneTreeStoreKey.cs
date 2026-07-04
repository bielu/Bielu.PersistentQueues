using System;
using System.Buffers.Binary;
using System.Text;
using ZoneTree.Comparers;
using ZoneTree.Serializers;

namespace Bielu.PersistentQueues.Storage.ZoneTree;

internal enum ZoneTreeStoreKeyKind : byte
{
    Schema = 0,
    Queue = 1,
    Message = 2
}

internal readonly record struct ZoneTreeStoreKey(
    ZoneTreeStoreKeyKind Kind,
    string QueueName,
    Guid MessageId)
{
    public static ZoneTreeStoreKey SchemaVersion() => new(ZoneTreeStoreKeyKind.Schema, string.Empty, Guid.Empty);

    public static ZoneTreeStoreKey Queue(string queueName) => new(ZoneTreeStoreKeyKind.Queue, queueName, Guid.Empty);

    public static ZoneTreeStoreKey Message(string queueName, Guid messageId) =>
        new(ZoneTreeStoreKeyKind.Message, queueName, messageId);

    public static ZoneTreeStoreKey FirstMessage(string queueName) =>
        new(ZoneTreeStoreKeyKind.Message, queueName, Guid.Empty);
}

internal sealed class ZoneTreeStoreKeyComparer : IRefComparer<ZoneTreeStoreKey>
{
    public int Compare(in ZoneTreeStoreKey x, in ZoneTreeStoreKey y)
    {
        var kind = x.Kind.CompareTo(y.Kind);
        if (kind != 0)
            return kind;

        var queue = string.CompareOrdinal(x.QueueName, y.QueueName);
        if (queue != 0)
            return queue;

        return x.MessageId.CompareTo(y.MessageId);
    }
}

internal sealed class ZoneTreeStoreKeySerializer : ISerializer<ZoneTreeStoreKey>
{
    public Memory<byte> Serialize(in ZoneTreeStoreKey entry)
    {
        var queueName = entry.QueueName ?? string.Empty;
        var queueByteCount = Encoding.UTF8.GetByteCount(queueName);
        var bytes = new byte[1 + sizeof(int) + queueByteCount + 16];
        var span = bytes.AsSpan();

        span[0] = (byte)entry.Kind;
        BinaryPrimitives.WriteInt32LittleEndian(span.Slice(1, sizeof(int)), queueByteCount);
        Encoding.UTF8.GetBytes(queueName, span.Slice(1 + sizeof(int), queueByteCount));
        entry.MessageId.TryWriteBytes(span.Slice(1 + sizeof(int) + queueByteCount, 16));

        return bytes;
    }

    public ZoneTreeStoreKey Deserialize(Memory<byte> bytes)
    {
        var span = bytes.Span;
        var kind = (ZoneTreeStoreKeyKind)span[0];
        var queueByteCount = BinaryPrimitives.ReadInt32LittleEndian(span.Slice(1, sizeof(int)));
        var queueName = Encoding.UTF8.GetString(span.Slice(1 + sizeof(int), queueByteCount));
        var messageId = new Guid(span.Slice(1 + sizeof(int) + queueByteCount, 16));

        return new ZoneTreeStoreKey(kind, queueName, messageId);
    }
}
