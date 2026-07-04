using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using ZoneTree;
using ZoneTree.Serializers;
using Bielu.PersistentQueues.Serialization;

namespace Bielu.PersistentQueues.Storage.ZoneTree;

/// <summary>
/// ZoneTree-based implementation of <see cref="IMessageStore"/> using a single ordered keyspace.
/// </summary>
public class ZoneTreeMessageStore : IMessageStore
{
    private const string OutgoingQueue = "outgoing";

    private static readonly Memory<byte> MetadataValue = new(new byte[] { 1 });

    private readonly ReaderWriterLockSlim _lifecycleLock;
    private readonly string _dataDirectory;
    private readonly IMessageSerializer _serializer;
    private readonly ZoneTreeStorageOptions _options;
    private readonly IZoneTree<ZoneTreeStoreKey, Memory<byte>> _tree;
    private readonly IMaintainer? _maintainer;
    private bool _treeDisposed;
    private bool _disposed;

    /// <summary>
    /// Creates a ZoneTreeMessageStore that uses the given data directory and message serializer with default storage options.
    /// </summary>
    /// <param name="dataDirectory">Root directory where ZoneTree data will be created and persisted.</param>
    /// <param name="serializer">Serializer used to convert Message instances to and from their persisted byte representation.</param>
    public ZoneTreeMessageStore(string dataDirectory, IMessageSerializer serializer)
        : this(dataDirectory, serializer, null)
    {
    }

    /// <summary>
    /// Initializes a ZoneTree-backed message store rooted at the given data directory.
    /// </summary>
    /// <param name="dataDirectory">Filesystem path used to store data and metadata.</param>
    /// <param name="serializer">Serializer used to serialize and deserialize stored messages.</param>
    /// <param name="options">Configuration for ZoneTree behavior; when null, default options are applied.</param>
    public ZoneTreeMessageStore(string dataDirectory, IMessageSerializer serializer, ZoneTreeStorageOptions? options)
    {
        _lifecycleLock = new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion);
        _dataDirectory = dataDirectory;
        _serializer = serializer;
        _options = options ?? new ZoneTreeStorageOptions();

        Directory.CreateDirectory(_dataDirectory);

        _tree = CreateStoreTree();
        if (_options.EnableMaintainer)
        {
            _maintainer = _tree.CreateMaintainer();
        }

        try
        {
            new ZoneTreeLegacyMigrator(_dataDirectory, _options, _tree).MigrateIfNeeded();
            CreateQueue(OutgoingQueue);
        }
        catch
        {
            try
            {
                DisposeOpenedTree();
            }
            catch
            {
                // Preserve the migration/opening failure as the primary exception.
            }

            throw;
        }
    }

    /// <summary>
    /// Gets the data directory path for this store.
    /// </summary>
    public string Path => _dataDirectory;

    /// <summary>
    /// Begins a new best-effort batch transaction.
    /// </summary>
    /// <returns>An <see cref="IStoreTransaction"/> representing the batch.</returns>
    public IStoreTransaction BeginTransaction()
    {
        CheckDisposed();
        return new ZoneTreeTransaction(this, ExecuteTransactionOperations);
    }

    /// <summary>
    /// Ensures a persistent queue with the given name exists.
    /// </summary>
    /// <param name="queueName">Logical name of the queue to create.</param>
    public void CreateQueue(string queueName)
    {
        ArgumentNullException.ThrowIfNull(queueName);
        CheckDisposed();

        _lifecycleLock.EnterWriteLock();
        try
        {
            CheckDisposed();
            _tree.Upsert(ZoneTreeStoreKey.Queue(queueName), MetadataValue);
        }
        finally
        {
            _lifecycleLock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Persists the provided incoming messages into their respective queues.
    /// </summary>
    /// <param name="messages">Messages to persist.</param>
    public void StoreIncoming(params IEnumerable<Message> messages)
    {
        CheckDisposed();

        _lifecycleLock.EnterReadLock();
        try
        {
            CheckDisposed();
            foreach (var group in messages.GroupBy(m => RequireQueueName(m.QueueString)))
            {
                EnsureQueueExistsNoLock(group.Key);
                foreach (var message in group)
                {
                    StoreMessageNoLock(group.Key, message);
                }
            }
        }
        finally
        {
            _lifecycleLock.ExitReadLock();
        }
    }

    /// <summary>
    /// Schedules per-message upsert operations into the given transaction.
    /// </summary>
    /// <param name="transaction">The transaction batch to which operations will be added.</param>
    /// <param name="messages">Messages to persist.</param>
    public void StoreIncoming(IStoreTransaction transaction, params IEnumerable<Message> messages)
    {
        CheckDisposed();
        var tx = GetZoneTreeTransaction(transaction);
        foreach (var group in messages.GroupBy(m => RequireQueueName(m.QueueString)))
        {
            var queueName = group.Key;
            foreach (var message in group)
            {
                var capturedMessage = message;
                tx.AddOperation(() =>
                {
                    EnsureQueueExistsNoLock(queueName);
                    StoreMessageNoLock(queueName, capturedMessage);
                });
            }
        }
    }

    /// <summary>
    /// Stores incoming raw wire-format messages without full deserialization.
    /// </summary>
    /// <param name="messages">Pre-parsed message info from WireFormatSplitter.</param>
    /// <param name="count">Number of messages to store.</param>
    /// <param name="serializer">Unused; raw bytes are stored directly.</param>
    public void StoreRawIncoming(RawMessageInfo[] messages, int count, IMessageSerializer serializer)
    {
        CheckDisposed();
        if (count == 0)
            return;

        _lifecycleLock.EnterReadLock();
        try
        {
            CheckDisposed();
            for (var i = 0; i < count; i++)
            {
                var info = messages[i];
                var queueName = WireFormatSplitter.GetQueueName(in info);
                EnsureQueueExistsNoLock(queueName);
                var messageId = new Guid(info.MessageId.Span);
                _tree.Upsert(ZoneTreeStoreKey.Message(queueName, messageId), info.FullMessage.ToArray());
            }
        }
        finally
        {
            _lifecycleLock.ExitReadLock();
        }
    }

    /// <summary>
    /// Deletes the persisted entries for the specified incoming messages from their respective queues.
    /// </summary>
    /// <param name="messages">Messages to delete.</param>
    public void DeleteIncoming(params IEnumerable<Message> messages)
    {
        CheckDisposed();

        _lifecycleLock.EnterReadLock();
        try
        {
            CheckDisposed();
            foreach (var group in messages.GroupBy(m => RequireQueueName(m.QueueString)))
            {
                EnsureQueueExistsNoLock(group.Key);
                foreach (var message in group)
                {
                    DeleteMessageNoLock(group.Key, message.Id.MessageIdentifier);
                }
            }
        }
        finally
        {
            _lifecycleLock.ExitReadLock();
        }
    }

    /// <summary>
    /// Provides an enumerable that streams all persisted messages for the specified queue.
    /// </summary>
    /// <param name="queueName">The logical queue name whose persisted messages will be enumerated.</param>
    /// <returns>An enumerable of persisted messages.</returns>
    public IEnumerable<Message> PersistedIncoming(string queueName)
    {
        CheckDisposed();
        return new ZoneTreeMessageEnumerable(this, queueName);
    }

    /// <summary>
    /// Enumerates messages currently persisted in the dedicated outgoing queue.
    /// </summary>
    /// <returns>An enumerable of persisted outgoing messages.</returns>
    public IEnumerable<Message> PersistedOutgoing()
    {
        CheckDisposed();
        return new ZoneTreeMessageEnumerable(this, OutgoingQueue);
    }

    /// <summary>
    /// Enumerates raw persisted entries from the dedicated outgoing queue.
    /// </summary>
    /// <returns>An enumerable of raw outgoing entries.</returns>
    public IEnumerable<RawOutgoingMessage> PersistedOutgoingRaw()
    {
        CheckDisposed();

        _lifecycleLock.EnterReadLock();
        try
        {
            CheckDisposed();
            EnsureQueueExistsNoLock(OutgoingQueue);
            using var iterator = _tree.CreateIterator();
            iterator.Seek(ZoneTreeStoreKey.FirstMessage(OutgoingQueue));
            while (iterator.Next())
            {
                var key = iterator.CurrentKey;
                if (!IsMessageKeyForQueue(key, OutgoingQueue))
                    break;

                var value = iterator.CurrentValue;
                if (value.Length == 0)
                    continue;

                var messageBytes = value.ToArray();
                var raw = WireFormatReader.ReadOutgoingMessage(new ReadOnlyMemory<byte>(messageBytes));
                var keyBytes = new byte[16];
                key.MessageId.TryWriteBytes(keyBytes);

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
            _lifecycleLock.ExitReadLock();
        }
    }

    /// <summary>
    /// Deletes persisted outgoing messages by raw identifiers.
    /// </summary>
    /// <param name="messageIds">Raw message identifiers to remove.</param>
    public void SuccessfullySentByIds(IEnumerable<ReadOnlyMemory<byte>> messageIds)
    {
        CheckDisposed();

        _lifecycleLock.EnterReadLock();
        try
        {
            CheckDisposed();
            EnsureQueueExistsNoLock(OutgoingQueue);
            foreach (var messageId in messageIds)
            {
                AtomicDeleteMessageNoLock(OutgoingQueue, new Guid(messageId.Span));
            }
        }
        finally
        {
            _lifecycleLock.ExitReadLock();
        }
    }

    /// <summary>
    /// Schedules moving the specified persisted message into another queue as part of the given transaction batch.
    /// </summary>
    /// <param name="transaction">The transaction batch that will execute the move.</param>
    /// <param name="queueName">Destination queue name.</param>
    /// <param name="message">The message to move.</param>
    public void MoveToQueue(IStoreTransaction transaction, string queueName, Message message)
    {
        CheckDisposed();
        var tx = GetZoneTreeTransaction(transaction);
        var capturedMessage = message;
        tx.AddOperation(() =>
        {
            var sourceQueue = RequireQueueName(capturedMessage.QueueString);
            EnsureQueueExistsNoLock(sourceQueue);
            EnsureQueueExistsNoLock(queueName);

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
                capturedMessage.Headers,
                partitionKey: capturedMessage.PartitionKey);

            StoreMessageNoLock(queueName, updatedMessage);
            DeleteMessageNoLock(sourceQueue, key);
        });
    }

    /// <summary>
    /// Schedules moving each specified message into another queue.
    /// </summary>
    /// <param name="transaction">The transaction batch to which move operations will be added.</param>
    /// <param name="queueName">Destination queue name.</param>
    /// <param name="messages">Messages to move.</param>
    public void MoveToQueue(IStoreTransaction transaction, string queueName, IEnumerable<Message> messages)
    {
        CheckDisposed();
        foreach (var message in messages)
        {
            MoveToQueue(transaction, queueName, message);
        }
    }

    /// <summary>
    /// Schedules removal of the specified message from its originating queue.
    /// </summary>
    /// <param name="transaction">The transaction batch to which the delete operation will be added.</param>
    /// <param name="message">The message to remove.</param>
    public void SuccessfullyReceived(IStoreTransaction transaction, Message message)
    {
        CheckDisposed();
        var tx = GetZoneTreeTransaction(transaction);
        var capturedMessage = message;
        tx.AddOperation(() =>
        {
            var queueName = RequireQueueName(capturedMessage.QueueString);
            EnsureQueueExistsNoLock(queueName);
            DeleteMessageNoLock(queueName, capturedMessage.Id.MessageIdentifier);
        });
    }

    /// <summary>
    /// Schedules removal of the specified messages from their queues.
    /// </summary>
    /// <param name="transaction">The transaction batch to which removal operations will be added.</param>
    /// <param name="messages">Messages to remove.</param>
    public void SuccessfullyReceived(IStoreTransaction transaction, IEnumerable<Message> messages)
    {
        CheckDisposed();
        foreach (var message in messages)
        {
            SuccessfullyReceived(transaction, message);
        }
    }

    /// <summary>
    /// Schedules storing an outgoing message as part of the provided transaction batch.
    /// </summary>
    /// <param name="transaction">The transaction batch to which the upsert operation will be added.</param>
    /// <param name="message">The outgoing message to persist.</param>
    public void StoreOutgoing(IStoreTransaction transaction, Message message)
    {
        CheckDisposed();
        var tx = GetZoneTreeTransaction(transaction);
        var capturedMessage = message;
        tx.AddOperation(() =>
        {
            EnsureQueueExistsNoLock(OutgoingQueue);
            StoreMessageNoLock(OutgoingQueue, capturedMessage);
        });
    }

    /// <summary>
    /// Persists a single outgoing message.
    /// </summary>
    /// <param name="message">The outgoing message to persist.</param>
    public void StoreOutgoing(Message message)
    {
        CheckDisposed();

        _lifecycleLock.EnterReadLock();
        try
        {
            CheckDisposed();
            EnsureQueueExistsNoLock(OutgoingQueue);
            StoreMessageNoLock(OutgoingQueue, message);
        }
        finally
        {
            _lifecycleLock.ExitReadLock();
        }
    }

    /// <summary>
    /// Persists multiple outgoing messages.
    /// </summary>
    /// <param name="messages">Messages to persist.</param>
    public void StoreOutgoing(params IEnumerable<Message> messages)
    {
        CheckDisposed();

        _lifecycleLock.EnterReadLock();
        try
        {
            CheckDisposed();
            EnsureQueueExistsNoLock(OutgoingQueue);
            foreach (var message in messages)
            {
                StoreMessageNoLock(OutgoingQueue, message);
            }
        }
        finally
        {
            _lifecycleLock.ExitReadLock();
        }
    }

    /// <summary>
    /// Persists multiple outgoing messages from a span.
    /// </summary>
    /// <param name="messages">Messages to persist.</param>
    public void StoreOutgoing(ReadOnlySpan<Message> messages)
    {
        CheckDisposed();

        _lifecycleLock.EnterReadLock();
        try
        {
            CheckDisposed();
            EnsureQueueExistsNoLock(OutgoingQueue);
            foreach (var message in messages)
            {
                StoreMessageNoLock(OutgoingQueue, message);
            }
        }
        finally
        {
            _lifecycleLock.ExitReadLock();
        }
    }

    /// <summary>
    /// Processes outgoing messages after a failed send.
    /// </summary>
    /// <param name="shouldRemove">When true, deletes matching outgoing entries immediately.</param>
    /// <param name="messages">Messages to evaluate.</param>
    public void FailedToSend(bool shouldRemove = false, params IEnumerable<Message> messages)
    {
        CheckDisposed();

        _lifecycleLock.EnterReadLock();
        try
        {
            CheckDisposed();
            EnsureQueueExistsNoLock(OutgoingQueue);
            foreach (var message in messages)
            {
                var key = ZoneTreeStoreKey.Message(OutgoingQueue, message.Id.MessageIdentifier);

                if (shouldRemove)
                {
                    _tree.AtomicUpsert(key, Memory<byte>.Empty);
                    continue;
                }

                _tree.TryAtomicGetAndUpdate(key, out _, (ref Memory<byte> storedValue) =>
                {
                    if (storedValue.Length == 0)
                        return false;

                    var storedMessage = _serializer.ToMessage(storedValue.Span);
                    if (message.SentAttempts >= message.MaxAttempts)
                    {
                        storedValue = Memory<byte>.Empty;
                        return true;
                    }

                    if (storedMessage.DeliverBy.HasValue)
                    {
                        var expire = storedMessage.DeliverBy.Value;
                        if (expire != DateTime.MinValue && DateTime.Now >= expire)
                        {
                            storedValue = Memory<byte>.Empty;
                            return true;
                        }

                        return false;
                    }

                    storedValue = _serializer.AsSpan(storedMessage).ToArray();
                    return true;
                });
            }
        }
        finally
        {
            _lifecycleLock.ExitReadLock();
        }
    }

    /// <summary>
    /// Deletes the persisted entries for the given outgoing messages.
    /// </summary>
    /// <param name="messages">Outgoing messages whose stored entries should be removed.</param>
    public void SuccessfullySent(params IEnumerable<Message> messages)
    {
        CheckDisposed();

        _lifecycleLock.EnterReadLock();
        try
        {
            CheckDisposed();
            EnsureQueueExistsNoLock(OutgoingQueue);
            foreach (var message in messages)
            {
                AtomicDeleteMessageNoLock(OutgoingQueue, message.Id.MessageIdentifier);
            }
        }
        finally
        {
            _lifecycleLock.ExitReadLock();
        }
    }

    /// <summary>
    /// Retrieves a persisted message by queue and message identifier.
    /// </summary>
    /// <param name="queueName">Queue name to search.</param>
    /// <param name="messageId">Message identifier to retrieve.</param>
    /// <returns>The deserialized message if found; otherwise null.</returns>
    public Message? GetMessage(string queueName, MessageId messageId)
    {
        CheckDisposed();

        _lifecycleLock.EnterReadLock();
        try
        {
            CheckDisposed();
            EnsureQueueExistsNoLock(queueName);
            if (_tree.TryGet(ZoneTreeStoreKey.Message(queueName, messageId.MessageIdentifier), out var value))
            {
                if (value.Length == 0)
                    return null;

                return _serializer.ToMessage(value.Span);
            }

            return null;
        }
        finally
        {
            _lifecycleLock.ExitReadLock();
        }
    }

    /// <summary>
    /// Enumerates all existing persistent queue names, excluding the dedicated outgoing queue.
    /// </summary>
    /// <returns>An array of queue names present in the store.</returns>
    public string[] GetAllQueues()
    {
        CheckDisposed();

        _lifecycleLock.EnterReadLock();
        try
        {
            CheckDisposed();
            var queues = new List<string>();
            using var iterator = _tree.CreateIterator();
            iterator.Seek(ZoneTreeStoreKey.Queue(string.Empty));
            while (iterator.Next())
            {
                var key = iterator.CurrentKey;
                if (key.Kind != ZoneTreeStoreKeyKind.Queue)
                    break;
                if (!string.Equals(key.QueueName, OutgoingQueue, StringComparison.Ordinal))
                {
                    queues.Add(key.QueueName);
                }
            }

            return queues.ToArray();
        }
        finally
        {
            _lifecycleLock.ExitReadLock();
        }
    }

    /// <summary>
    /// Deletes all persisted messages from every queue managed by this store.
    /// </summary>
    public void ClearAllStorage()
    {
        CheckDisposed();

        _lifecycleLock.EnterReadLock();
        try
        {
            CheckDisposed();
            var keysToDelete = new List<ZoneTreeStoreKey>();
            using (var iterator = _tree.CreateIterator())
            {
                iterator.Seek(ZoneTreeStoreKey.FirstMessage(string.Empty));
                while (iterator.Next())
                {
                    var key = iterator.CurrentKey;
                    if (key.Kind != ZoneTreeStoreKeyKind.Message)
                        break;
                    keysToDelete.Add(key);
                }
            }

            foreach (var key in keysToDelete)
            {
                _tree.ForceDelete(key);
            }
        }
        finally
        {
            _lifecycleLock.ExitReadLock();
        }
    }

    /// <summary>
    /// Returns the number of persisted messages currently stored in the specified queue.
    /// </summary>
    /// <param name="queueName">Queue name whose stored message count to retrieve.</param>
    /// <returns>The number of messages in the queue.</returns>
    public long GetMessageCount(string queueName)
    {
        CheckDisposed();

        _lifecycleLock.EnterReadLock();
        try
        {
            CheckDisposed();
            EnsureQueueExistsNoLock(queueName);
            var count = 0L;
            using var iterator = _tree.CreateIterator();
            iterator.Seek(ZoneTreeStoreKey.FirstMessage(queueName));
            while (iterator.Next())
            {
                if (!IsMessageKeyForQueue(iterator.CurrentKey, queueName))
                    break;
                if (iterator.CurrentValue.Length == 0)
                    continue;

                count++;
            }

            return count;
        }
        finally
        {
            _lifecycleLock.ExitReadLock();
        }
    }

    /// <summary>
    /// Removes a persistent queue and all messages stored in it.
    /// </summary>
    /// <param name="queueName">Queue name to delete.</param>
    public void DeleteQueue(string queueName)
    {
        CheckDisposed();

        if (string.Equals(queueName, OutgoingQueue, StringComparison.Ordinal))
            throw new InvalidOperationException(
                $"Cannot delete the reserved '{OutgoingQueue}' queue. It is required for outgoing message operations.");

        _lifecycleLock.EnterWriteLock();
        try
        {
            CheckDisposed();
            if (!_tree.ContainsKey(ZoneTreeStoreKey.Queue(queueName)))
                return;

            var keysToDelete = new List<ZoneTreeStoreKey>();
            using (var iterator = _tree.CreateIterator())
            {
                iterator.Seek(ZoneTreeStoreKey.FirstMessage(queueName));
                while (iterator.Next())
                {
                    var key = iterator.CurrentKey;
                    if (!IsMessageKeyForQueue(key, queueName))
                        break;
                    keysToDelete.Add(key);
                }
            }

            foreach (var key in keysToDelete)
            {
                _tree.ForceDelete(key);
            }

            _tree.ForceDelete(ZoneTreeStoreKey.Queue(queueName));
        }
        finally
        {
            _lifecycleLock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Releases managed resources used by the ZoneTreeMessageStore.
    /// </summary>
    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    private void Dispose(bool disposing)
    {
        if (!disposing)
            return;

        _lifecycleLock.EnterWriteLock();
        try
        {
            if (_disposed)
                return;

            _disposed = true;
            try
            {
                DisposeOpenedTree();
            }
            catch
            {
                // Swallow exceptions during disposal.
            }
        }
        finally
        {
            _lifecycleLock.ExitWriteLock();
        }
    }

    private void ExecuteTransactionOperations(Action operations)
    {
        _lifecycleLock.EnterReadLock();
        try
        {
            CheckDisposed();
            operations();
        }
        finally
        {
            _lifecycleLock.ExitReadLock();
        }
    }

    private void StoreMessageNoLock(string queueName, Message message)
    {
        _tree.Upsert(
            ZoneTreeStoreKey.Message(queueName, message.Id.MessageIdentifier),
            _serializer.AsSpan(message).ToArray());
    }

    private void DeleteMessageNoLock(string queueName, Guid messageId)
    {
        _tree.ForceDelete(ZoneTreeStoreKey.Message(queueName, messageId));
    }

    private void AtomicDeleteMessageNoLock(string queueName, Guid messageId)
    {
        _tree.AtomicUpsert(ZoneTreeStoreKey.Message(queueName, messageId), Memory<byte>.Empty);
    }

    private void EnsureQueueExistsNoLock(string queueName)
    {
        if (!_tree.ContainsKey(ZoneTreeStoreKey.Queue(queueName)))
            throw new QueueDoesNotExistException(queueName);
    }

    private ZoneTreeTransaction GetZoneTreeTransaction(IStoreTransaction transaction)
    {
        if (transaction is not ZoneTreeTransaction zt)
            throw new ArgumentException(
                $"Expected ZoneTreeTransaction but received {transaction.GetType().Name}",
                nameof(transaction));
        if (!ReferenceEquals(zt.Owner, this))
            throw new ArgumentException(
                "Transaction belongs to a different ZoneTreeMessageStore instance.",
                nameof(transaction));
        return zt;
    }

    private void CheckDisposed()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(ZoneTreeMessageStore),
                "Cannot perform operation on a disposed message store");
    }

    private IZoneTree<ZoneTreeStoreKey, Memory<byte>> CreateStoreTree()
    {
        var storePath = System.IO.Path.Combine(_dataDirectory, ZoneTreeLegacyMigrator.StoreDirectoryName);
        Directory.CreateDirectory(storePath);

        var factory = new ZoneTreeFactory<ZoneTreeStoreKey, Memory<byte>>()
            .SetDataDirectory(storePath)
            .SetComparer(new ZoneTreeStoreKeyComparer())
            .SetKeySerializer(new ZoneTreeStoreKeySerializer())
            .SetValueSerializer(new ByteArraySerializer())
            .SetMutableSegmentMaxItemCount(_options.MutableSegmentMaxItemCount)
            .SetDiskSegmentMaxItemCount(_options.DiskSegmentMaxItemCount)
            .SetIsDeletedDelegate((in ZoneTreeStoreKey _, in Memory<byte> value) => value.Length == 0)
            .SetMarkValueDeletedDelegate((ref Memory<byte> value) => value = Memory<byte>.Empty);

        return factory.OpenOrCreate();
    }

    private static bool IsMessageKeyForQueue(ZoneTreeStoreKey key, string queueName)
    {
        return key.Kind == ZoneTreeStoreKeyKind.Message &&
            string.Equals(key.QueueName, queueName, StringComparison.Ordinal);
    }

    private static string RequireQueueName(string? queueName)
    {
        if (queueName is null)
            throw new QueueDoesNotExistException("unknown");
        return queueName;
    }

    private void DisposeOpenedTree()
    {
        if (_treeDisposed)
            return;

        _treeDisposed = true;

        if (_maintainer != null)
        {
            _maintainer.TryCancelBackgroundThreads();
            _maintainer.WaitForBackgroundThreads();
            _maintainer.Dispose();
        }

        _tree.Maintenance.SaveMetaData();
        _tree.Dispose();
    }

    private class ZoneTreeMessageEnumerable(ZoneTreeMessageStore store, string queueName) : IEnumerable<Message>
    {
        public IEnumerator<Message> GetEnumerator() => new ZoneTreeMessageEnumerator(store, queueName);

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
    }

    private class ZoneTreeMessageEnumerator : IEnumerator<Message>
    {
        private readonly ZoneTreeMessageStore _store;
        private readonly string _queueName;
        private IZoneTreeIterator<ZoneTreeStoreKey, Memory<byte>>? _iterator;
        private bool _disposed;

        public ZoneTreeMessageEnumerator(ZoneTreeMessageStore store, string queueName)
        {
            _store = store;
            _queueName = queueName;
            Initialize();
        }

        public Message Current { get; private set; }

        object System.Collections.IEnumerator.Current => Current;

        public bool MoveNext()
        {
            if (_disposed || _iterator == null)
                return false;

            try
            {
                while (_iterator.Next())
                {
                    var key = _iterator.CurrentKey;
                    if (!IsMessageKeyForQueue(key, _queueName))
                        return false;

                    var value = _iterator.CurrentValue;
                    if (value.Length == 0)
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

        public void Dispose()
        {
            if (_disposed)
                return;

            Cleanup();
            _disposed = true;
        }

        private void Initialize()
        {
            _store._lifecycleLock.EnterReadLock();
            try
            {
                _store.CheckDisposed();
                _store.EnsureQueueExistsNoLock(_queueName);
                _iterator = _store._tree.CreateIterator();
                _iterator.Seek(ZoneTreeStoreKey.FirstMessage(_queueName));
            }
            catch
            {
                Cleanup();
                throw;
            }
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
                    if (_store._lifecycleLock.IsReadLockHeld)
                    {
                        _store._lifecycleLock.ExitReadLock();
                    }
                }
                catch (SynchronizationLockException)
                {
                    // Lock was already released.
                }

                _iterator = null;
            }
        }
    }
}
