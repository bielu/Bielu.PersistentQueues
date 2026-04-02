using System;

namespace Bielu.PersistentQueues;

/// <summary>
/// Represents a storage size with support for convenient unit conversions (bytes, megabytes, gigabytes).
/// Use this type when configuring storage sizes like LMDB MapSize to avoid manual byte calculations.
/// </summary>
/// <example>
/// <code>
/// // Using factory methods
/// var size = StorageSize.FromMegabytes(100);
/// var size = StorageSize.FromGigabytes(2);
///
/// // Using shorthand
/// var size = StorageSize.MB(100);
/// var size = StorageSize.GB(2);
/// </code>
/// </example>
public readonly struct StorageSize : IEquatable<StorageSize>, IComparable<StorageSize>
{
    private const long BytesPerMegabyte = 1024L * 1024;
    private const long BytesPerGigabyte = 1024L * 1024 * 1024;

    /// <summary>
    /// Gets the size in bytes.
    /// </summary>
    public long Bytes { get; }

    private StorageSize(long bytes)
    {
        if (bytes < 0)
            throw new ArgumentOutOfRangeException(nameof(bytes), bytes, "Storage size cannot be negative.");

        Bytes = bytes;
    }

    /// <summary>
    /// Creates a <see cref="StorageSize"/> from the specified number of bytes.
    /// </summary>
    /// <param name="bytes">The size in bytes.</param>
    public static StorageSize FromBytes(long bytes) => new(bytes);

    /// <summary>
    /// Creates a <see cref="StorageSize"/> from the specified number of megabytes.
    /// </summary>
    /// <param name="megabytes">The size in megabytes.</param>
    public static StorageSize FromMegabytes(long megabytes)
    {
        if (megabytes < 0)
            throw new ArgumentOutOfRangeException(nameof(megabytes), megabytes, "Storage size cannot be negative.");

        return new StorageSize(megabytes * BytesPerMegabyte);
    }

    /// <summary>
    /// Creates a <see cref="StorageSize"/> from the specified number of gigabytes.
    /// </summary>
    /// <param name="gigabytes">The size in gigabytes.</param>
    public static StorageSize FromGigabytes(long gigabytes)
    {
        if (gigabytes < 0)
            throw new ArgumentOutOfRangeException(nameof(gigabytes), gigabytes, "Storage size cannot be negative.");

        return new StorageSize(gigabytes * BytesPerGigabyte);
    }

    /// <summary>
    /// Shorthand for <see cref="FromMegabytes"/>.
    /// </summary>
    /// <param name="megabytes">The size in megabytes.</param>
    public static StorageSize MB(long megabytes) => FromMegabytes(megabytes);

    /// <summary>
    /// Shorthand for <see cref="FromGigabytes"/>.
    /// </summary>
    /// <param name="gigabytes">The size in gigabytes.</param>
    public static StorageSize GB(long gigabytes) => FromGigabytes(gigabytes);

    /// <summary>
    /// Implicit conversion to <see cref="long"/> (bytes).
    /// </summary>
    public static implicit operator long(StorageSize size) => size.Bytes;

    /// <summary>
    /// Returns a human-readable representation of the storage size.
    /// </summary>
    public override string ToString()
    {
        if (Bytes >= BytesPerGigabyte && Bytes % BytesPerGigabyte == 0)
            return $"{Bytes / BytesPerGigabyte} GB";
        if (Bytes >= BytesPerMegabyte && Bytes % BytesPerMegabyte == 0)
            return $"{Bytes / BytesPerMegabyte} MB";
        return $"{Bytes} bytes";
    }

    public bool Equals(StorageSize other) => Bytes == other.Bytes;
    public override bool Equals(object? obj) => obj is StorageSize other && Equals(other);
    public override int GetHashCode() => Bytes.GetHashCode();
    public int CompareTo(StorageSize other) => Bytes.CompareTo(other.Bytes);

    public static bool operator ==(StorageSize left, StorageSize right) => left.Equals(right);
    public static bool operator !=(StorageSize left, StorageSize right) => !left.Equals(right);
    public static bool operator <(StorageSize left, StorageSize right) => left.CompareTo(right) < 0;
    public static bool operator >(StorageSize left, StorageSize right) => left.CompareTo(right) > 0;
    public static bool operator <=(StorageSize left, StorageSize right) => left.CompareTo(right) <= 0;
    public static bool operator >=(StorageSize left, StorageSize right) => left.CompareTo(right) >= 0;
}
