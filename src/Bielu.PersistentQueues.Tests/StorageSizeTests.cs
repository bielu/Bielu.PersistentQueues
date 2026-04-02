using System;
using Shouldly;
using Xunit;

namespace Bielu.PersistentQueues.Tests;

public class StorageSizeTests
{
    [Fact]
    public void FromBytes_returns_exact_byte_count()
    {
        var size = StorageSize.FromBytes(12345);
        size.Bytes.ShouldBe(12345);
    }

    [Fact]
    public void FromMegabytes_converts_to_bytes()
    {
        var size = StorageSize.FromMegabytes(100);
        size.Bytes.ShouldBe(100L * 1024 * 1024);
    }

    [Fact]
    public void FromGigabytes_converts_to_bytes()
    {
        var size = StorageSize.FromGigabytes(2);
        size.Bytes.ShouldBe(2L * 1024 * 1024 * 1024);
    }

    [Fact]
    public void MB_shorthand_converts_to_bytes()
    {
        var size = StorageSize.MB(500);
        size.Bytes.ShouldBe(500L * 1024 * 1024);
    }

    [Fact]
    public void GB_shorthand_converts_to_bytes()
    {
        var size = StorageSize.GB(1);
        size.Bytes.ShouldBe(1L * 1024 * 1024 * 1024);
    }

    [Fact]
    public void Implicit_conversion_to_long_returns_bytes()
    {
        StorageSize size = StorageSize.MB(100);
        long bytes = size;
        bytes.ShouldBe(100L * 1024 * 1024);
    }

    [Fact]
    public void Negative_bytes_throws_ArgumentOutOfRangeException()
    {
        Should.Throw<ArgumentOutOfRangeException>(() => StorageSize.FromBytes(-1));
    }

    [Fact]
    public void Negative_megabytes_throws_ArgumentOutOfRangeException()
    {
        Should.Throw<ArgumentOutOfRangeException>(() => StorageSize.FromMegabytes(-1));
    }

    [Fact]
    public void Negative_gigabytes_throws_ArgumentOutOfRangeException()
    {
        Should.Throw<ArgumentOutOfRangeException>(() => StorageSize.FromGigabytes(-1));
    }

    [Fact]
    public void ToString_displays_GB_for_whole_gigabytes()
    {
        StorageSize.GB(2).ToString().ShouldBe("2 GB");
    }

    [Fact]
    public void ToString_displays_MB_for_whole_megabytes()
    {
        StorageSize.MB(100).ToString().ShouldBe("100 MB");
    }

    [Fact]
    public void ToString_displays_bytes_for_non_round_values()
    {
        StorageSize.FromBytes(12345).ToString().ShouldBe("12345 bytes");
    }

    [Fact]
    public void Equality_comparison_works()
    {
        var a = StorageSize.MB(100);
        var b = StorageSize.FromBytes(100L * 1024 * 1024);
        a.ShouldBe(b);
        (a == b).ShouldBeTrue();
        (a != b).ShouldBeFalse();
    }

    [Fact]
    public void Comparison_operators_work()
    {
        var small = StorageSize.MB(100);
        var large = StorageSize.GB(1);
        (small < large).ShouldBeTrue();
        (large > small).ShouldBeTrue();
        (small <= large).ShouldBeTrue();
        (large >= small).ShouldBeTrue();
    }

    [Fact]
    public void Zero_size_is_valid()
    {
        var size = StorageSize.FromBytes(0);
        size.Bytes.ShouldBe(0);
    }
}
