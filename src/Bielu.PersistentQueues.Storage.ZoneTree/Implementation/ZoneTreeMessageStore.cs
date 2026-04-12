using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using Tenray.ZoneTree;
using Tenray.ZoneTree.Comparers;
using Tenray.ZoneTree.Serializers;
using Bielu.PersistentQueues.Serialization;

namespace Bielu.PersistentQueues.Storage.ZoneTree;

/// <summary>
/// ZoneTree-based implementation of <see cref="IMessageStore"/>.
/// Uses a separate ZoneTree instance per queue, with Guid keys and Memory&lt;byte&gt; values.
/// Uses ZoneTree's built-in ByteArraySerializer for value serialization.
/// </summary>
public class ZoneTreeMessageStore : IMessageStore
{
    private const string OutgoingQueue = "outgoing";

    /// <summary>
    /// File name stored in each queue subdirectory to record the original queue name,
    /// so that names containing path-invalid characters can survive a restart round-trip.
    /// </summary>
    private const string QueueNameMetadataFile = ".queue-name";

    private readonly ReaderWriterLockSlim _lock;
    private readonly string _dataDirectory;
    private readonly IMessageSerializer _serializer;
    private readonly ZoneTreeStorageOptions _options;
    private readonly ConcurrentDictionary<string, IZoneTree<Guid, Memory<byte>>> _trees;
    private readonly ConcurrentDictionary<string, IMaintainer> _maintainers;
    private bool _disposed;

    public ZoneTreeMessageStore(string dataDirectory, IMessageSerializer serializer)
        : this(dataDirectory, serializer, null)
    {
    }

    public ZoneTreeMessageStore(string dataDirectory, IMessageSerializer serializer,
        ZoneTreeStorageOptions? options)
    {
        _lock = new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion);
        _dataDirectory = dataDirectory;
        _serializer = serializer;
        _options = options ?? new ZoneTreeStorageOptions();
        _trees = new ConcurrentDictionary<string, IZoneTree<Guid, Memory<byte>>>();
        _maintainers = new ConcurrentDictionary<string, IMaintainer>();

        Directory.CreateDirectory(_dataDirectory);

        // Re-open any existing queue trees from prior sessions
        ReopenExistingQueues();

        // Ensure the outgoing queue always exists
        CreateQueue(OutgoingQueue);
    }

    /// <summary>
    /// Gets the data directory path for this store.
    /// </summary>
    public string Path => _dataDirectory;

    public IStoreTransaction BeginTransaction()
    {
        CheckDisposed();

        _lock.EnterWriteLock();
        try
        {
            return new ZoneTreeTransaction(_lock);
        }
        catch
        {
            _lock.ExitWriteLock();
            throw;
        }
    }

    public void CreateQueue(string queueName)
    {
        CheckDisposed();

        _lock.EnterWriteLock();
        try
        {
            if (_trees.ContainsKey(queueName))
                return;

            var tree = CreateZoneTree(queueName);
            _trees.TryAdd(queueName, tree);

            if (_options.EnableMaintainer)
            {
                var maintainer = tree.CreateMaintainer();
                _maintainers.TryAdd(queueName, maintainer);
            }
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    public void StoreIncoming(params IEnumerable<Message> messages)
    {
        CheckDisposed();

        _lock.EnterWriteLock();
        try
        {
            foreach (var group in messages.GroupBy(m => m.QueueString))
            {
                var queueName = group.Key!;
                var tree = GetTree(queueName);
                foreach (var message in group)
                {
                    var key = message.Id.MessageIdentifier;
                    Memory<byte> value = _serializer.AsSpan(message).ToArray();
                    tree.Upsert(key, value);
                }
            }
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    public void StoreIncoming(IStoreTransaction transaction, params IEnumerable<Message> messages)
    {
        CheckDisposed();
        var tx = GetZoneTreeTransaction(transaction);
        foreach (var group in messages.GroupBy(m => m.QueueString))
        {
            var queueName = group.Key!;
            foreach (var message in group)
            {
                var capturedMessage = message;
                var capturedQueue = queueName;
                tx.AddOperation(() =>
                {
                    var tree = GetTree(capturedQueue);
                    var key = capturedMessage.Id.MessageIdentifier;
                    Memory<byte> value = _serializer.AsSpan(capturedMessage).ToArray();
                    tree.Upsert(key, value);
                });
            }
        }
    }

    public void DeleteIncoming(params IEnumerable<Message> messages)
    {
        CheckDisposed();

        _lock.EnterWriteLock();
        try
        {
            foreach (var group in messages.GroupBy(m => m.QueueString))
            {
                var queueName = group.Key!;
                var tree = GetTree(queueName);
                foreach (var message in group)
                {
                    tree.ForceDelete(message.Id.MessageIdentifier);
                }
            }
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    public IEnumerable<Message> PersistedIncoming(string queueName)
    {
        CheckDisposed();
        return new ZoneTreeMessageEnumerable(this, queueName);
    }

    public IEnumerable<Message> PersistedOutgoing()
    {
        CheckDisposed();
        return new ZoneTreeMessageEnumerable(this, OutgoingQueue);
    }

    /// <summary>
    /// Returns outgoing messages as raw wire-format bytes with routing information extracted.
    /// This enables zero-copy sending via the TCP Sender.
    /// </summary>
    public IEnumerable<RawOutgoingMessage> PersistedOutgoingRaw()
    {
        CheckDisposed();

        _lock.EnterReadLock();
        try
        {
            var tree = GetTree(OutgoingQueue);
            using var iterator = tree.CreateIterator();
            while (iterator.Next())
            {
                var value = iterator.CurrentValue;
                if (value.Length == 0) // Skip deleted entries
                    continue;

                // value is the full serialized message; extract routing information
                var messageBytes = value.ToArray();
                var raw = WireFormatReader.ReadOutgoingMessage(new ReadOnlyMemory<byte>(messageBytes));

                // Override MessageId with the actual GUID key bytes
                var keyBytes = new byte[16];
                iterator.CurrentKey.TryWriteBytes(keyBytes);

                yield return new RawOutgoingMessage
                {
                    MessageId = keyBytes,
                    DestinationUriBytes = raw.DestinationUriBytes,
                    QueueNameBytes = raw.QueueNameBytes,
                    FullMessage = messageBytes
                };
            }
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    /// <summary>
    /// Deletes outgoing messages by their raw 16-byte MessageId keys.
    /// </summary>
    public void SuccessfullySentByIds(IEnumerable<ReadOnlyMemory<byte>> messageIds)
    {
        CheckDisposed();

        _lock.EnterWriteLock();
        try
        {
            var tree = GetTree(OutgoingQueue);
            foreach (var messageId in messageIds)
            {
                var guid = new Guid(messageId.Span);
                tree.ForceDelete(guid);
            }
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    public void MoveToQueue(IStoreTransaction transaction, string queueName, Message message)
    {
        CheckDisposed();
        var tx = GetZoneTreeTransaction(transaction);
        var capturedMessage = message;
        tx.AddOperation(() =>
        {
            var sourceTree = GetTree(capturedMessage.QueueString!);
            var targetTree = GetTree(queueName);
            var key = capturedMessage.Id.MessageIdentifier;

            var updatedMessage = new Message(
                capturedMessage.Id,
                capturedMessage.Data,
                queueName.AsMemory(),
                capturedMessage.SentAt,
                capturedMessage.SubQueue,
                capturedMessage.DestinationUri,
                capturedMessage.DeliverBy,
                capturedMessage.MaxAttempts,
                capturedMessage.Headers
            );
            Memory<byte> value = _serializer.AsSpan(updatedMessage).ToArray();

            // Write to target FIRST, then delete from source.
            // Worst case on failure: duplicate message (safe), never lost message.
            targetTree.Upsert(key, value);
            sourceTree.ForceDelete(key);
        });
    }

    public void MoveToQueue(IStoreTransaction transaction, string queueName, IEnumerable<Message> messages)
    {
        CheckDisposed();
        foreach (var message in messages)
        {
            MoveToQueue(transaction, queueName, message);
        }
    }

    public void SuccessfullyReceived(IStoreTransaction transaction, Message message)
    {
        CheckDisposed();
        var tx = GetZoneTreeTransaction(transaction);
        var capturedMessage = message;
        tx.AddOperation(() =>
        {
            var tree = GetTree(capturedMessage.QueueString!);
            tree.ForceDelete(capturedMessage.Id.MessageIdentifier);
        });
    }

    public void SuccessfullyReceived(IStoreTransaction transaction, IEnumerable<Message> messages)
    {
        CheckDisposed();
        foreach (var message in messages)
        {
            SuccessfullyReceived(transaction, message);
        }
    }

    public void StoreOutgoing(IStoreTransaction transaction, Message message)
    {
        CheckDisposed();
        var tx = GetZoneTreeTransaction(transaction);
        var capturedMessage = message;
        tx.AddOperation(() =>
        {
            var tree = GetTree(OutgoingQueue);
            var key = capturedMessage.Id.MessageIdentifier;
            Memory<byte> value = _serializer.AsSpan(capturedMessage).ToArray();
            tree.Upsert(key, value);
        });
    }

    public void StoreOutgoing(Message message)
    {
        CheckDisposed();

        _lock.EnterWriteLock();
        try
        {
            var tree = GetTree(OutgoingQueue);
            var key = message.Id.MessageIdentifier;
            Memory<byte> value = _serializer.AsSpan(message).ToArray();
            tree.Upsert(key, value);
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    public void StoreOutgoing(params IEnumerable<Message> messages)
    {
        CheckDisposed();

        _lock.EnterWriteLock();
        try
        {
            var tree = GetTree(OutgoingQueue);
            foreach (var message in messages)
            {
                var key = message.Id.MessageIdentifier;
                Memory<byte> value = _serializer.AsSpan(message).ToArray();
                tree.Upsert(key, value);
            }
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    public void StoreOutgoing(ReadOnlySpan<Message> messages)
    {
        CheckDisposed();

        _lock.EnterWriteLock();
        try
        {
            var tree = GetTree(OutgoingQueue);
            foreach (var message in messages)
            {
                var key = message.Id.MessageIdentifier;
                Memory<byte> value = _serializer.AsSpan(message).ToArray();
                tree.Upsert(key, value);
            }
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    public void FailedToSend(bool shouldRemove = false, params IEnumerable<Message> messages)
    {
        CheckDisposed();

        _lock.EnterWriteLock();
        try
        {
            var tree = GetTree(OutgoingQueue);
            foreach (var message in messages)
            {
                var key = message.Id.MessageIdentifier;

                // Honor shouldRemove flag: when true, remove immediately
                // (e.g., permanently-failed messages like HostNotFound)
                if (shouldRemove)
                {
                    tree.ForceDelete(key);
                    continue;
                }

                if (!tree.TryGet(key, out var storedValue))
                    continue;

                var msg = _serializer.ToMessage(storedValue.Span);
                var attempts = message.SentAttempts;
                if (attempts >= message.MaxAttempts)
                {
                    tree.ForceDelete(key);
                }
                else if (msg.DeliverBy.HasValue)
                {
                    var expire = msg.DeliverBy.Value;
                    if (expire != DateTime.MinValue && DateTime.Now >= expire)
                    {
                        tree.ForceDelete(key);
                    }
                }
                else
                {
                    Memory<byte> value = _serializer.AsSpan(msg).ToArray();
                    tree.Upsert(key, value);
                }
            }
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    public void SuccessfullySent(params IEnumerable<Message> messages)
    {
        CheckDisposed();

        _lock.EnterWriteLock();
        try
        {
            var tree = GetTree(OutgoingQueue);
            foreach (var message in messages)
            {
                tree.ForceDelete(message.Id.MessageIdentifier);
            }
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    public Message? GetMessage(string queueName, MessageId messageId)
    {
        CheckDisposed();

        _lock.EnterReadLock();
        try
        {
            var tree = GetTree(queueName);
            if (tree.TryGet(messageId.MessageIdentifier, out var value))
            {
                return _serializer.ToMessage(value.Span);
            }

            return null;
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    public string[] GetAllQueues()
    {
        CheckDisposed();
        return _trees.Keys
            .Where(q => q != OutgoingQueue)
            .ToArray();
    }

    public void ClearAllStorage()
    {
        CheckDisposed();

        _lock.EnterWriteLock();
        try
        {
            foreach (var kvp in _trees)
            {
                var tree = kvp.Value;
                var keysToDelete = new List<Guid>();
                using (var iterator = tree.CreateIterator())
                {
                    while (iterator.Next())
                    {
                        keysToDelete.Add(iterator.CurrentKey);
                    }
                }

                foreach (var key in keysToDelete)
                {
                    tree.ForceDelete(key);
                }
            }
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    public long GetMessageCount(string queueName)
    {
        CheckDisposed();

        _lock.EnterReadLock();
        try
        {
            var tree = GetTree(queueName);
            return tree.Count();
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    public void DeleteQueue(string queueName)
    {
        CheckDisposed();

        _lock.EnterWriteLock();
        try
        {
            if (_maintainers.TryRemove(queueName, out var maintainer))
            {
                maintainer.Dispose();
            }

            if (_trees.TryRemove(queueName, out var tree))
            {
                tree.Maintenance.Drop();
                tree.Dispose();
            }

            var queuePath = GetQueuePath(queueName);
            if (Directory.Exists(queuePath))
            {
                Directory.Delete(queuePath, true);
            }
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);
        Dispose(true);
    }

    private void Dispose(bool disposing)
    {
        if (_disposed)
            return;

        _disposed = true;

        if (disposing)
        {
            _lock.EnterWriteLock();
            try
            {
                foreach (var kvp in _maintainers)
                {
                    try
                    {
                        kvp.Value.Dispose();
                    }
                    catch
                    {
                        // Swallow exceptions during disposal
                    }
                }

                _maintainers.Clear();

                foreach (var kvp in _trees)
                {
                    try
                    {
                        kvp.Value.Maintenance.SaveMetaData();
                        kvp.Value.Dispose();
                    }
                    catch
                    {
                        // Swallow exceptions during disposal
                    }
                }

                _trees.Clear();
            }
            finally
            {
                _lock.ExitWriteLock();
                _lock.Dispose();
            }
        }
    }

    private void CheckDisposed()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(ZoneTreeMessageStore),
                "Cannot perform operation on a disposed message store");
    }

    private static ZoneTreeTransaction GetZoneTreeTransaction(IStoreTransaction transaction) =>
        transaction is ZoneTreeTransaction zt
            ? zt
            : throw new ArgumentException(
                $"Expected ZoneTreeTransaction but received {transaction.GetType().Name}",
                nameof(transaction));

    private IZoneTree<Guid, Memory<byte>> GetTree(string queueName)
    {
        if (_trees.TryGetValue(queueName, out var tree))
            return tree;

        throw new QueueDoesNotExistException(queueName);
    }

    /// <summary>
    /// Encodes a queue name as a URL-safe directory name.
    /// Characters that are not alphanumeric, hyphen, or underscore are percent-encoded.
    /// This ensures the encoding is fully reversible for round-trip queue reopening.
    /// </summary>
    private static string EncodeQueueName(string queueName)
    {
        var sb = new StringBuilder(queueName.Length);
        foreach (var c in queueName)
        {
            if (char.IsLetterOrDigit(c) || c == '-' || c == '_' || c == '.')
            {
                sb.Append(c);
            }
            else
            {
                // Percent-encode: each byte of the UTF-8 representation gets %XX
                foreach (var b in Encoding.UTF8.GetBytes(new[] { c }))
                {
                    sb.Append('%');
                    sb.Append(b.ToString("X2"));
                }
            }
        }
        return sb.ToString();
    }

    /// <summary>
    /// Decodes a percent-encoded directory name back to the original queue name.
    /// </summary>
    private static string DecodeQueueName(string encoded)
    {
        var bytes = new List<byte>();
        for (var i = 0; i < encoded.Length; i++)
        {
            if (encoded[i] == '%' && i + 2 < encoded.Length)
            {
                var hex = encoded.Substring(i + 1, 2);
                bytes.Add(Convert.ToByte(hex, 16));
                i += 2;
            }
            else
            {
                // Flush accumulated percent-encoded bytes as UTF-8
                // before appending the literal character
                bytes.AddRange(Encoding.UTF8.GetBytes(new[] { encoded[i] }));
            }
        }
        return Encoding.UTF8.GetString(bytes.ToArray());
    }

    private string GetQueuePath(string queueName)
    {
        var safeName = EncodeQueueName(queueName);
        return System.IO.Path.Combine(_dataDirectory, safeName);
    }

    private IZoneTree<Guid, Memory<byte>> CreateZoneTree(string queueName)
    {
        var queuePath = GetQueuePath(queueName);
        Directory.CreateDirectory(queuePath);

        // Persist the original queue name so ReopenExistingQueues can recover it
        var metaPath = System.IO.Path.Combine(queuePath, QueueNameMetadataFile);
        if (!File.Exists(metaPath))
        {
            File.WriteAllText(metaPath, queueName);
        }

        var factory = new ZoneTreeFactory<Guid, Memory<byte>>()
            .SetDataDirectory(queuePath)
            .SetComparer(new GuidComparerAscending())
            .SetKeySerializer(new StructSerializer<Guid>())
            .SetValueSerializer(new ByteArraySerializer())
            .SetMutableSegmentMaxItemCount(_options.MutableSegmentMaxItemCount)
            .SetDiskSegmentMaxItemCount(_options.DiskSegmentMaxItemCount)
            .SetIsDeletedDelegate((in Guid _, in Memory<byte> value) => value.Length == 0)
            .SetMarkValueDeletedDelegate((ref Memory<byte> value) => value = Memory<byte>.Empty);

        return factory.OpenOrCreate();
    }

    private void ReopenExistingQueues()
    {
        if (!Directory.Exists(_dataDirectory))
            return;

        foreach (var dir in Directory.GetDirectories(_dataDirectory))
        {
            // Recover the original queue name from metadata file, falling back to decoding
            var metaPath = System.IO.Path.Combine(dir, QueueNameMetadataFile);
            string queueName;
            if (File.Exists(metaPath))
            {
                queueName = File.ReadAllText(metaPath).Trim();
            }
            else
            {
                queueName = DecodeQueueName(System.IO.Path.GetFileName(dir));
            }

            if (_trees.ContainsKey(queueName))
                continue;

            try
            {
                var tree = CreateZoneTree(queueName);
                _trees.TryAdd(queueName, tree);

                if (_options.EnableMaintainer)
                {
                    var maintainer = tree.CreateMaintainer();
                    _maintainers.TryAdd(queueName, maintainer);
                }
            }
            catch
            {
                // Skip directories that aren't valid ZoneTree stores
            }
        }
    }

    /// <summary>
    /// Streaming enumerable that iterates over ZoneTree entries and deserializes messages.
    /// </summary>
    private class ZoneTreeMessageEnumerable : IEnumerable<Message>
    {
        private readonly ZoneTreeMessageStore _store;
        private readonly string _queueName;

        public ZoneTreeMessageEnumerable(ZoneTreeMessageStore store, string queueName)
        {
            _store = store;
            _queueName = queueName;
        }

        public IEnumerator<Message> GetEnumerator() => new ZoneTreeMessageEnumerator(_store, _queueName);

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
    }

    private class ZoneTreeMessageEnumerator : IEnumerator<Message>
    {
        private readonly ZoneTreeMessageStore _store;
        private readonly string _queueName;
        private IZoneTreeIterator<Guid, Memory<byte>>? _iterator;
        private bool _disposed;

        public ZoneTreeMessageEnumerator(ZoneTreeMessageStore store, string queueName)
        {
            _store = store;
            _queueName = queueName;
            Initialize();
        }

        private void Initialize()
        {
            _store._lock.EnterReadLock();
            try
            {
                var tree = _store.GetTree(_queueName);
                _iterator = tree.CreateIterator();
            }
            catch
            {
                Cleanup();
                throw;
            }
        }

        public Message Current { get; private set; }

        object System.Collections.IEnumerator.Current => Current;

        public bool MoveNext()
        {
            if (_disposed || _iterator == null)
                return false;

            try
            {
                // Use iteration instead of recursion to skip deleted entries
                // to avoid stack overflow with many consecutive tombstones
                while (_iterator.Next())
                {
                    var value = _iterator.CurrentValue;
                    if (value.Length == 0) // Skip deleted entries
                        continue;
                    Current = _store._serializer.ToMessage(value.Span);
                    return true;
                }
            }
            catch
            {
                return false;
            }

            return false;
        }

        public void Reset()
        {
            Cleanup();
            Initialize();
        }

        private void Cleanup()
        {
            try
            {
                _iterator?.Dispose();
            }
            finally
            {
                try
                {
                    if (_store._lock.IsReadLockHeld)
                    {
                        _store._lock.ExitReadLock();
                    }
                }
                catch (SynchronizationLockException)
                {
                    // Lock was already released
                }

                _iterator = null;
            }
        }

        public void Dispose()
        {
            if (!_disposed)
            {
                Cleanup();
                _disposed = true;
            }
        }
    }
}
