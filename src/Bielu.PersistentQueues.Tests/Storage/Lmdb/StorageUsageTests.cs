using Bielu.PersistentQueues.Storage.LMDB;
using Shouldly;
using Xunit;
using Xunit.Abstractions;

namespace Bielu.PersistentQueues.Tests.Storage.Lmdb;

public class StorageUsageTests(ITestOutputHelper output) : TestBase(output)
{

    [Fact]
    public void get_storage_usage_info_returns_non_null()
    {
        StorageScenario(store =>
        {
            var info = store.GetStorageUsageInfo();
            info.ShouldNotBeNull();
        });
    }

    [Fact]
    public void get_storage_usage_info_reports_correct_total_bytes()
    {
        StorageScenario(store =>
        {
            var info = store.GetStorageUsageInfo();
            info.ShouldNotBeNull();
            // Configured with MapSize = 1024 * 1024 * 100 (100 MB) in TestBase
            info.Value.TotalBytes.ShouldBe(1024 * 1024 * 100);
        });
    }

    [Fact]
    public void get_storage_usage_info_reports_non_zero_used_bytes()
    {
        StorageScenario(store =>
        {
            var info = store.GetStorageUsageInfo();
            info.ShouldNotBeNull();
            // Even an empty LMDB environment uses some pages for metadata
            info.Value.UsedBytes.ShouldBeGreaterThan(0);
        });
    }

    [Fact]
    public void get_storage_usage_info_usage_percentage_is_between_0_and_100()
    {
        StorageScenario(store =>
        {
            var info = store.GetStorageUsageInfo();
            info.ShouldNotBeNull();
            info.Value.UsagePercentage.ShouldBeGreaterThanOrEqualTo(0.0);
            info.Value.UsagePercentage.ShouldBeLessThanOrEqualTo(100.0);
        });
    }

    [Fact]
    public void storage_usage_increases_after_storing_messages()
    {
        StorageScenario(store =>
        {
            var infoBefore = store.GetStorageUsageInfo();
            infoBefore.ShouldNotBeNull();

            // Store a batch of messages to ensure measurable growth
            for (int i = 0; i < 100; i++)
            {
                store.StoreIncoming(NewMessage("test", new string('x', 1024)));
            }

            var infoAfter = store.GetStorageUsageInfo();
            infoAfter.ShouldNotBeNull();

            infoAfter.Value.UsedBytes.ShouldBeGreaterThan(infoBefore.Value.UsedBytes);
            infoAfter.Value.UsagePercentage.ShouldBeGreaterThan(infoBefore.Value.UsagePercentage);
        });
    }
}
