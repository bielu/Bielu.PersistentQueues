using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using ZoneTree;
using ZoneTree.Comparers;
using ZoneTree.Serializers;

namespace Bielu.PersistentQueues.Storage.ZoneTree;

internal sealed class ZoneTreeLegacyMigrator(
    string dataDirectory,
    ZoneTreeStorageOptions options,
    IZoneTree<ZoneTreeStoreKey, Memory<byte>> targetTree)
{
    internal const string StoreDirectoryName = "__bielu_single_store";

    private const string QueueNameMetadataFile = ".queue-name";
    private const byte SchemaVersion = 2;

    private static readonly Memory<byte> MetadataValue = new(new byte[] { 1 });
    private static readonly Memory<byte> SchemaVersionValue = new(new[] { SchemaVersion });

    public void MigrateIfNeeded()
    {
        if (targetTree.ContainsKey(ZoneTreeStoreKey.SchemaVersion()))
            return;

        var failures = new List<Exception>();
        foreach (var dir in Directory.GetDirectories(dataDirectory))
        {
            if (string.Equals(Path.GetFileName(dir), StoreDirectoryName, StringComparison.Ordinal))
            {
                continue;
            }

            try
            {
                if (!LooksLikeLegacyQueueDirectory(dir))
                    continue;

                MigrateDirectory(dir);
            }
            catch (Exception ex)
            {
                failures.Add(new InvalidOperationException(
                    $"Failed to migrate legacy ZoneTree queue directory '{dir}'.",
                    ex));
            }
        }

        if (failures.Count > 0)
        {
            throw new InvalidOperationException(
                "Failed to migrate one or more legacy ZoneTree queue directories. " +
                "The new schema marker was not written so migration can be retried.",
                new AggregateException(failures));
        }

        targetTree.Upsert(ZoneTreeStoreKey.SchemaVersion(), SchemaVersionValue);
        targetTree.Maintenance.SaveMetaData();
    }

    private static bool LooksLikeLegacyQueueDirectory(string directory)
    {
        return File.Exists(Path.Combine(directory, QueueNameMetadataFile)) ||
            Directory.EnumerateFiles(directory, "*.json", SearchOption.TopDirectoryOnly).Any();
    }

    private void MigrateDirectory(string directory)
    {
        var queueName = GetQueueName(directory);
        if (string.IsNullOrEmpty(queueName))
        {
            throw new InvalidDataException(
                $"Legacy ZoneTree queue directory '{directory}' does not contain a valid queue name.");
        }

        using var legacyTree = OpenLegacyQueueTree(directory);
        targetTree.Upsert(ZoneTreeStoreKey.Queue(queueName), MetadataValue);

        using var iterator = legacyTree.CreateIterator();
        while (iterator.Next())
        {
            var value = iterator.CurrentValue;
            if (value.Length == 0)
                continue;

            targetTree.Upsert(
                ZoneTreeStoreKey.Message(queueName, iterator.CurrentKey),
                value.ToArray());
        }
    }

    private static string GetQueueName(string directory)
    {
        var metaPath = Path.Combine(directory, QueueNameMetadataFile);
        return File.Exists(metaPath)
            ? File.ReadAllText(metaPath).TrimEnd('\r', '\n')
            : DecodeQueueName(Path.GetFileName(directory));
    }

    private IZoneTree<Guid, Memory<byte>> OpenLegacyQueueTree(string queuePath)
    {
        var factory = new ZoneTreeFactory<Guid, Memory<byte>>()
            .SetDataDirectory(queuePath)
            .SetComparer(new GuidComparerAscending())
            .SetKeySerializer(new StructSerializer<Guid>())
            .SetValueSerializer(new ByteArraySerializer())
            .SetMutableSegmentMaxItemCount(options.MutableSegmentMaxItemCount)
            .SetDiskSegmentMaxItemCount(options.DiskSegmentMaxItemCount)
            .SetIsDeletedDelegate((in Guid _, in Memory<byte> value) => value.Length == 0)
            .SetMarkValueDeletedDelegate((ref Memory<byte> value) => value = Memory<byte>.Empty);

        return factory.OpenOrCreate();
    }

    private static string DecodeQueueName(string encoded)
    {
        var chars = new StringBuilder(encoded.Length);
        var bytes = new List<byte>();

        for (var i = 0; i < encoded.Length; i++)
        {
            if (encoded[i] == '%' &&
                i + 2 < encoded.Length &&
                byte.TryParse(encoded.AsSpan(i + 1, 2), NumberStyles.HexNumber, null, out var value))
            {
                bytes.Add(value);
                i += 2;
                continue;
            }

            FlushDecodedBytes(chars, bytes);
            chars.Append(encoded[i]);
        }

        FlushDecodedBytes(chars, bytes);
        return chars.ToString();
    }

    private static void FlushDecodedBytes(StringBuilder chars, List<byte> bytes)
    {
        if (bytes.Count == 0)
            return;

        chars.Append(Encoding.UTF8.GetString(bytes.ToArray()));
        bytes.Clear();
    }
}
