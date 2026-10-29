using System;
using System.Buffers.Binary;
using System.IO;
using System.Security.Cryptography;
using System.Text;

namespace Bielu.PersistentQueues.Cluster;

/// <summary>
/// Signs and verifies cluster frames with HMAC-SHA256 over a shared secret.
/// </summary>
/// <remarks>
/// A sealed frame is the payload followed by an 8-byte UTC timestamp (Unix milliseconds) and a
/// 32-byte MAC over the channel, the payload and the timestamp. The channel keeps a frame from
/// one exchange (for example a replication response) from being accepted as another, and the
/// timestamp bounds how long a captured frame can be replayed. Without a secret, frames are
/// passed through unchanged.
/// </remarks>
internal sealed class ClusterAuthenticator
{
    internal const int TrailerSize = sizeof(long) + HMACSHA256.HashSizeInBytes;
    internal static readonly TimeSpan MaxClockSkew = TimeSpan.FromMinutes(5);

    private readonly byte[]? _key;
    private readonly TimeProvider _timeProvider;

    public ClusterAuthenticator(string? sharedSecret, TimeProvider? timeProvider = null)
    {
        _key = string.IsNullOrEmpty(sharedSecret) ? null : Encoding.UTF8.GetBytes(sharedSecret);
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    /// <summary>Gets an authenticator that passes frames through unchanged.</summary>
    public static ClusterAuthenticator None { get; } = new(null);

    public bool Enabled => _key != null;

    /// <summary>Appends the timestamp and MAC to <paramref name="payload"/>.</summary>
    public byte[] Seal(ClusterChannel channel, byte[] payload)
    {
        if (_key is not { } key)
            return payload;

        var frame = new byte[payload.Length + TrailerSize];
        payload.CopyTo(frame, 0);
        BinaryPrimitives.WriteInt64LittleEndian(frame.AsSpan(payload.Length),
            _timeProvider.GetUtcNow().ToUnixTimeMilliseconds());
        ComputeMac(key, channel, frame.AsSpan(0, payload.Length + sizeof(long)), frame.AsSpan(payload.Length + sizeof(long)));
        return frame;
    }

    /// <summary>Verifies a sealed frame and returns its payload.</summary>
    /// <exception cref="InvalidDataException">Thrown when the frame is unsigned, forged or outside the allowed clock skew.</exception>
    public ReadOnlyMemory<byte> Open(ClusterChannel channel, ReadOnlyMemory<byte> frame)
    {
        if (_key is not { } key)
            return frame;

        if (frame.Length < TrailerSize)
            throw new InvalidDataException("Cluster frame is not signed.");

        var payloadLength = frame.Length - TrailerSize;
        var signed = frame.Span[..(payloadLength + sizeof(long))];
        Span<byte> expected = stackalloc byte[HMACSHA256.HashSizeInBytes];
        ComputeMac(key, channel, signed, expected);
        if (!CryptographicOperations.FixedTimeEquals(expected, frame.Span[(payloadLength + sizeof(long))..]))
            throw new InvalidDataException("Cluster frame signature is invalid.");

        var sentAt = DateTimeOffset.FromUnixTimeMilliseconds(BinaryPrimitives.ReadInt64LittleEndian(signed[payloadLength..]));
        if ((_timeProvider.GetUtcNow() - sentAt).Duration() > MaxClockSkew)
            throw new InvalidDataException($"Cluster frame timestamp {sentAt:O} is outside the allowed clock skew.");

        return frame[..payloadLength];
    }

    private static void ComputeMac(byte[] key, ClusterChannel channel, ReadOnlySpan<byte> signed, Span<byte> destination)
    {
        using var hmac = IncrementalHash.CreateHMAC(HashAlgorithmName.SHA256, key);
        hmac.AppendData([(byte)channel]);
        hmac.AppendData(signed);
        hmac.GetHashAndReset(destination);
    }
}

/// <summary>
/// The exchange a cluster frame belongs to; part of the signed data.
/// </summary>
internal enum ClusterChannel : byte
{
    Gossip = 1,
    ReplicationRequest = 2,
    ReplicationResponse = 3
}
