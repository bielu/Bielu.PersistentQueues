using System;
using Tenray.ZoneTree.Serializers;

namespace Bielu.PersistentQueues.Storage.ZoneTree;

/// <summary>
/// Serializer for byte arrays compatible with ZoneTree's ISerializer&lt;byte[]&gt;.
/// ZoneTree's built-in ByteArraySerializer implements ISerializer&lt;Memory&lt;byte&gt;&gt;,
/// so this provides a byte[] version.
/// </summary>
internal sealed class ByteArrayZoneTreeSerializer : ISerializer<byte[]>
{
    public byte[] Deserialize(Memory<byte> bytes)
    {
        return bytes.ToArray();
    }

    public Memory<byte> Serialize(in byte[] entry)
    {
        return new Memory<byte>(entry);
    }
}
