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

    /// <summary>
    /// Creates a ZoneTreeMessageStore that uses the given data directory and message serializer with default storage options.
    /// </summary>
    /// <param name="dataDirectory">Root directory where per-queue data directories will be created and persisted.</param>
    /// <param name="serializer">Serializer used to convert Message instances to and from their persisted byte representation.</param>
    public ZoneTreeMessageStore(string dataDirectory, IMessageSerializer serializer)
        : this(dataDirectory, serializer, null)
    {
    }

    /// <summary>
    /// Initializes a ZoneTree-based message store rooted at the given data directory, restoring any previously persisted queues and ensuring the dedicated outgoing queue exists.
    /// </summary>
    /// <param name="dataDirectory">Filesystem path used to store per-queue data and metadata; the directory will be created if it does not exist.</param>
    /// <param name="serializer">Serializer used to serialize and deserialize stored messages.</param>
    /// <param name="options">Configuration for ZoneTree behavior; when null, default options are applied.</param>
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

    /// <summary>
    /// Begins a new store transaction while acquiring the store's write lock for exclusive access.
    /// </summary>
    /// <returns>An <see cref="IStoreTransaction"/> representing the transaction; the store's write lock is held for the transaction's lifetime.</returns>
    public IStoreTransaction BeginTransaction()
    {
        CheckDisposed();

        _lock.EnterWriteLock();
        try
        {
            return new ZoneTreeTransaction(_lock, this);
        }
        catch
        {
            _lock.ExitWriteLock();
            throw;
        }
    }

    /// <summary>
    /// Ensures a persistent queue with the given name exists; if the queue is not already present it is created along with its ZoneTree and, when enabled, a maintainer.
    /// </summary>
    /// <param name="queueName">Logical name of the queue to create. The method is idempotent and returns without action if the queue already exists.</param>
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

    /// <summary>
    /// Persists the provided incoming messages into their respective queue stores.
    /// </summary>
    /// <param name="messages">One or more sequences of messages; messages are grouped by each message's <c>QueueString</c> and stored into the corresponding queue.</param>
    /// <exception cref="ObjectDisposedException">Thrown if the store has been disposed.</exception>
    /// <exception cref="QueueDoesNotExistException">Thrown if a message references a queue that does not exist.</exception>
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

    /// <summary>
    /// Schedules per-message upsert operations into the given transaction for each provided message, grouping messages by their QueueString.
    /// </summary>
    /// <param name="transaction">The transaction to which per-message upsert operations will be added.</param>
    /// <param name="messages">Messages to persist; each message is serialized and scheduled to be stored under its MessageId.MessageIdentifier in the queue identified by the message's QueueString.</param>
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

    /// <summary>
    /// Deletes the persisted entries for the specified incoming messages from their respective queue trees.
    /// </summary>
    /// <param name="messages">One or more collections of messages; messages are grouped by their <c>QueueString</c> and each message is removed by its <c>MessageId.MessageIdentifier</c>.</param>
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

    /// <summary>
    /// Provides an enumerable that streams all persisted messages for the specified queue.
    /// </summary>
    /// <param name="queueName">The logical queue name whose persisted messages will be enumerated.</param>
    /// <returns>An <see cref="IEnumerable{Message}"/> that yields persisted messages from the queue, deserializing entries on demand and skipping deleted entries.</returns>
    /// <exception cref="ObjectDisposedException">Thrown if the store has been disposed.</exception>
    public IEnumerable<Message> PersistedIncoming(string queueName)
    {
        CheckDisposed();
        return new ZoneTreeMessageEnumerable(this, queueName);
    }

    /// <summary>
    /// Enumerates messages currently persisted in the store's dedicated outgoing queue.
    /// </summary>
    /// <returns>An <see cref="IEnumerable{Message}"/> that yields each persisted outgoing message; deleted entries are excluded.</returns>
    public IEnumerable<Message> PersistedOutgoing()
    {
        CheckDisposed();
        return new ZoneTreeMessageEnumerable(this, OutgoingQueue);
    }

    /// <summary>
    /// Enumerates raw persisted entries from the store's dedicated outgoing queue.
    /// </summary>
    /// <returns>An enumerable of non-deleted outgoing entries where each <see cref="RawOutgoingMessage"/> contains the 16-byte message identifier, the destination URI bytes, the queue name bytes, and the complete serialized message payload.</returns>
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
    /// Deletes the persisted outgoing messages whose identifiers are provided.
    /// </summary>
    /// <param name="messageIds">A sequence of 16-byte GUID values (as ReadOnlyMemory&lt;byte&gt;) identifying persisted messages in the outgoing queue; entries matching these IDs will be removed if present.</param>
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

    /// <summary>
    /// Schedules moving the specified persisted message into another queue as part of the given transaction.
    /// </summary>
    /// <param name="transaction">The transaction that will execute the move; must be a ZoneTree-backed transaction from this store.</param>
    /// <param name="queueName">The destination queue name to assign to the message.</param>
    /// <param name="message">The message to move; the stored entry identified by the message's Id will be moved and its QueueString replaced with <paramref name="queueName"/>.</param>
    /// <exception cref="ObjectDisposedException">Thrown if the store has been disposed.</exception>
    /// <exception cref="ArgumentException">Thrown if <paramref name="transaction"/> is not a ZoneTreeTransaction produced by this store.</exception>
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

    /// <summary>
    /// Schedules operations on the provided transaction to move each specified message into the named queue when the transaction is executed.
    /// </summary>
    /// <param name="transaction">The transaction to which move operations will be added.</param>
    /// <param name="queueName">Destination queue name.</param>
    /// <param name="messages">Messages to move into the destination queue.</param>
    public void MoveToQueue(IStoreTransaction transaction, string queueName, IEnumerable<Message> messages)
    {
        CheckDisposed();
        foreach (var message in messages)
        {
            MoveToQueue(transaction, queueName, message);
        }
    }

    /// <summary>
    /// Schedules removal of the specified message from its originating queue as part of the provided transaction.
    /// </summary>
    /// <param name="transaction">The transaction to which the delete operation will be added.</param>
    /// <param name="message">The message to remove; its queue name and message identifier are used to locate the stored entry.</param>
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

    /// <summary>
    /// Records removal of the specified messages from their queues in the provided transaction.
    /// </summary>
    /// <param name="transaction">The transaction to which removal operations will be added.</param>
    /// <param name="messages">The messages to remove from storage when the transaction is executed.</param>
    public void SuccessfullyReceived(IStoreTransaction transaction, IEnumerable<Message> messages)
    {
        CheckDisposed();
        foreach (var message in messages)
        {
            SuccessfullyReceived(transaction, message);
        }
    }

    /// <summary>
    /// Schedules an upsert of the specified message into the store's dedicated outgoing queue as part of the provided transaction.
    /// </summary>
    /// <param name="transaction">The transaction to which the upsert operation will be added; must be a ZoneTree-backed transaction.</param>
    /// <param name="message">The message to store in the outgoing queue; its serialized form is written using the message's MessageIdentifier as the key.</param>
    /// <exception cref="ArgumentException">Thrown when <paramref name="transaction"/> is not a ZoneTreeTransaction.</exception>
    /// <exception cref="ObjectDisposedException">Thrown if the store has been disposed.</exception>
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

    /// <summary>
    /// Persists the given message into the store's dedicated outgoing queue, keyed by the message identifier.
    /// </summary>
    /// <param name="message">The message to serialize and store in the outgoing queue; an existing entry with the same message identifier will be replaced.</param>
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

    /// <summary>
    /// Stores the given messages in the store's dedicated outgoing queue as persisted entries.
    /// </summary>
    /// <param name="messages">Messages to persist into the outgoing queue; each message is serialized and upserted by its MessageId.</param>
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

    /// <summary>
    /// Stores the provided messages into the dedicated outgoing queue.
    /// </summary>
    /// <param name="messages">Messages to persist; each message is serialized and stored under its MessageId.MessageIdentifier, replacing any existing entry with the same identifier.</param>
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

    /// <summary>
    /// Processes persisted outgoing messages after a failed send and either removes or updates their stored entries according to retry and expiration rules.
    /// </summary>
    /// <remarks>
    /// For each provided message the store locates the persisted outgoing entry by the message's MessageId and:
    /// - removes the entry immediately if <paramref name="shouldRemove"/> is true;
    /// - removes the entry if the message's SentAttempts is greater than or equal to MaxAttempts;
    /// - removes the entry if the stored message has a DeliverBy timestamp that is not DateTime.MinValue and that timestamp has passed;
    /// - otherwise re-serializes and updates (re-enqueues) the stored message entry.
    /// </remarks>
    /// <param name="shouldRemove">When true, delete matching persisted outgoing entries immediately regardless of attempts or expiration.</param>
    /// <param name="messages">One or more sequences of messages whose corresponding persisted outgoing entries should be evaluated and acted upon.</param>
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

    /// <summary>
    /// Deletes the persisted entries for the given outgoing messages from the outgoing queue.
    /// </summary>
    /// <param name="messages">One or more collections of outgoing messages whose stored entries should be removed.</param>
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

    /// <summary>
    /// Retrieve the persisted message with the specified message identifier from the given queue.
    /// </summary>
    /// <param name="queueName">The name of the queue to search.</param>
    /// <param name="messageId">The identifier of the message to retrieve.</param>
    /// <returns>The deserialized <see cref="Message"/> if found; otherwise <c>null</c>.</returns>
    /// <exception cref="ObjectDisposedException">Thrown if the store has been disposed.</exception>
    /// <exception cref="QueueDoesNotExistException">Thrown if the specified queue does not exist.</exception>
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

    /// <summary>
    /// Enumerates all existing persistent queue names, excluding the dedicated outgoing queue.
    /// </summary>
    /// <returns>An array of queue names present in the store, excluding the "outgoing" queue.</returns>
    public string[] GetAllQueues()
    {
        CheckDisposed();
        return _trees.Keys
            .Where(q => q != OutgoingQueue)
            .ToArray();
    }

    /// <summary>
    /// Deletes all persisted messages from every queue managed by this store.
    /// </summary>
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

    /// <summary>
    /// Returns the number of persisted messages currently stored in the specified queue.
    /// </summary>
    /// <param name="queueName">The logical queue name whose stored message count to retrieve.</param>
    /// <returns>The number of messages in the queue.</returns>
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

    /// <summary>
    /// Removes a persisted queue and its associated resources from the store.
    /// </summary>
    /// <param name="queueName">The logical queue name to delete.</param>
    /// <remarks>
    /// If present, disposes and removes the queue's maintainer and ZoneTree, and deletes the queue's data directory.
    /// If the queue does not exist, the method returns without error.
    /// </remarks>
    /// <exception cref="ObjectDisposedException">Thrown if the store has been disposed.</exception>
    public void DeleteQueue(string queueName)
    {
        CheckDisposed();

        if (string.Equals(queueName, OutgoingQueue, StringComparison.Ordinal))
            throw new InvalidOperationException(
                $"Cannot delete the reserved '{OutgoingQueue}' queue. It is required for outgoing message operations.");

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

    /// <summary>
    /// Releases managed resources used by the ZoneTreeMessageStore.
    /// </summary>
    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Releases managed resources and persists per-queue metadata when the store is disposed.
    /// </summary>
    /// <remarks>
    /// If <paramref name="disposing"/> is true, the method sets the disposed flag, acquires the store's write lock,
    /// disposes and clears any maintainers, calls SaveMetaData and disposes each ZoneTree, clears the tree map,
    /// and finally releases and disposes the lock. Disposal of individual maintainers or trees ignores exceptions.
    /// </remarks>
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

    /// <summary>
    /// Validates that the message store has not been disposed.
    /// </summary>
    /// <exception cref="ObjectDisposedException">Thrown when the store has already been disposed.</exception>
    private void CheckDisposed()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(ZoneTreeMessageStore),
                "Cannot perform operation on a disposed message store");
    }

    /// <summary>
    /// Verify that an IStoreTransaction is a ZoneTreeTransaction owned by this store and return it.
    /// </summary>
    /// <param name="transaction">The transaction to validate and cast.</param>
    /// <returns>The given transaction cast to <see cref="ZoneTreeTransaction"/>.</returns>
    /// <exception cref="ArgumentException">Thrown when <paramref name="transaction"/> is not a <see cref="ZoneTreeTransaction"/> or belongs to a different store.</exception>
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

    /// <summary>
    /// Retrieve the ZoneTree instance backing the specified queue.
    /// </summary>
    /// <param name="queueName">The logical name of the queue.</param>
    /// <returns>The <see cref="IZoneTree{Guid,Memory{byte}}"/> instance for the given queue.</returns>
    /// <exception cref="QueueDoesNotExistException">Thrown when no tree exists for <paramref name="queueName"/>.</exception>
    private IZoneTree<Guid, Memory<byte>> GetTree(string queueName)
    {
        if (_trees.TryGetValue(queueName, out var tree))
            return tree;

        throw new QueueDoesNotExistException(queueName);
    }

    /// <summary>
    /// Produces a filesystem-safe directory name by percent-encoding characters outside ASCII letters, digits, '-', '_', and '.' using their UTF-8 byte values.
    /// Uses <see cref="System.Text.Rune"/> enumeration to correctly handle surrogate pairs (non-BMP characters).
    /// </summary>
    /// <param name="queueName">Original queue name to encode.</param>
    /// <returns>The encoded queue name where each unsafe character is replaced by one or more `%XX` sequences representing its UTF-8 bytes.</returns>
    private static string EncodeQueueName(string queueName)
    {
        var sb = new StringBuilder(queueName.Length);
        Span<byte> utf8 = stackalloc byte[4];
        foreach (var rune in queueName.EnumerateRunes())
        {
            if (rune.IsAscii && (char.IsLetterOrDigit((char)rune.Value) || rune.Value == '-' || rune.Value == '_' || rune.Value == '.'))
            {
                sb.Append((char)rune.Value);
            }
            else
            {
                // Percent-encode: each byte of the UTF-8 representation gets %XX
                int len = rune.EncodeToUtf8(utf8);
                for (int i = 0; i < len; i++)
                {
                    sb.Append('%');
                    sb.Append(utf8[i].ToString("X2"));
                }
            }
        }
        return sb.ToString();
    }

    /// <summary>
    /// Reconstructs an original queue name from its percent-encoded directory-safe representation.
    /// </summary>
    /// <param name="encoded">The percent-encoded queue name where non-safe characters are encoded as `%HH` sequences.</param>
    /// <returns>The decoded queue name as a UTF-8 string, with `%HH` sequences converted to the corresponding bytes and all other characters preserved.</returns>
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

    /// <summary>
    /// Produces the filesystem path for the given queue by encoding the queue name into a filesystem-safe directory name and combining it with the store's data directory.
    /// </summary>
    /// <param name="queueName">Original queue name to encode into a safe directory name.</param>
    /// <returns>Full directory path under the store's data directory where the queue's files are stored.</returns>
    private string GetQueuePath(string queueName)
    {
        var safeName = EncodeQueueName(queueName);
        return System.IO.Path.Combine(_dataDirectory, safeName);
    }

    /// <summary>
    /// Creates or opens a ZoneTree for the specified queue and ensures the queue directory and metadata file exist.
    /// </summary>
    /// <param name="queueName">The logical queue name to create or reopen; persisted to a metadata file so the queue can be recovered on restart.</param>
    /// <returns>An opened <c>IZoneTree&lt;Guid, Memory&lt;byte&gt;&gt;</c> configured for Guid keys, byte-array values, deleted-value semantics, and segment size limits from options.</returns>
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

    /// <summary>
    /// Reopens previously persisted queue stores found under the configured data directory and registers their trees and optional maintainers.
    /// </summary>
    /// <remarks>
    /// If the data directory does not exist this method returns immediately. For each subdirectory it attempts to recover the original queue name from the queue metadata file, falling back to decoding the directory name; invalid or non-ZoneTree directories are skipped.
    /// </remarks>
    private void ReopenExistingQueues()
    {
        if (!Directory.Exists(_dataDirectory))
            return;

        foreach (var dir in Directory.GetDirectories(_dataDirectory))
        {
            try
            {
                // Recover the original queue name from metadata file, falling back to decoding
                var metaPath = System.IO.Path.Combine(dir, QueueNameMetadataFile);
                string queueName;
                if (File.Exists(metaPath))
                {
                    queueName = File.ReadAllText(metaPath);
                    // Strip trailing line-ending characters (\r\n or \n) that File.WriteAllText may append
                    queueName = queueName.TrimEnd('\r', '\n');
                }
                else
                {
                    queueName = DecodeQueueName(System.IO.Path.GetFileName(dir));
                }

                if (string.IsNullOrEmpty(queueName) || _trees.ContainsKey(queueName))
                    continue;

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
                // Skip directories that aren't valid ZoneTree stores or have unreadable metadata
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

        /// <summary>
        /// Initializes a new instance of <see cref="ZoneTreeMessageEnumerable"/> for the specified queue.
        /// </summary>
        /// <param name="queueName">The name of the queue whose persisted messages will be enumerated.</param>
        public ZoneTreeMessageEnumerable(ZoneTreeMessageStore store, string queueName)
        {
            _store = store;
            _queueName = queueName;
        }

        /// <summary>
        /// Provides an enumerator that iterates persisted, non-deleted messages for the store's captured queue.
        /// </summary>
        /// <returns>An <see cref="IEnumerator{Message}"/> that enumerates persisted messages from the specified queue.</returns>
        public IEnumerator<Message> GetEnumerator() => new ZoneTreeMessageEnumerator(_store, _queueName);

        /// <summary>
        /// Get a non-generic enumerator that iterates through the messages in the queue.
        /// </summary>
        /// <returns>An <see cref="System.Collections.IEnumerator"/> that can be used to iterate the collection.</returns>
        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
    }

    private class ZoneTreeMessageEnumerator : IEnumerator<Message>
    {
        private readonly ZoneTreeMessageStore _store;
        private readonly string _queueName;
        private IZoneTreeIterator<Guid, Memory<byte>>? _iterator;
        private bool _disposed;

        /// <summary>
        /// Initializes a new <see cref="ZoneTreeMessageEnumerator"/> for the specified message store and queue and prepares it for enumeration.
        /// </summary>
        /// <param name="store">The owning <see cref="ZoneTreeMessageStore"/> that provides access to the queue.</param>
        /// <param name="queueName">The name of the queue whose messages will be enumerated.</param>
        public ZoneTreeMessageEnumerator(ZoneTreeMessageStore store, string queueName)
        {
            _store = store;
            _queueName = queueName;
            Initialize();
        }

        /// <summary>
        /// Acquires the store read lock and initializes the zone-tree iterator for the enumerator's queue.
        /// </summary>
        /// <remarks>
        /// On failure the method will clean up any partial state and rethrow the original exception.
        /// </remarks>
        /// <exception cref="Exception">Propagates any exception thrown while retrieving the tree or creating the iterator.</exception>
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

        /// <summary>
        /// Advances the enumerator to the next persisted message in the queue, skipping entries marked as deleted.
        /// </summary>
        /// <returns>`true` if the enumerator advanced and <see cref="Current"/> contains the next message; `false` if the enumerator is disposed, exhausted, an error occurred, or no next message is available.</returns>
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

        /// <summary>
        /// Resets the enumerator to the beginning of the underlying queue so iteration will start from the first message.
        /// </summary>
        public void Reset()
        {
            Cleanup();
            Initialize();
        }

        /// <summary>
        /// Disposes the current zone-tree iterator, releases the store's read lock if held, and clears the iterator reference.
        /// </summary>
        /// <remarks>
        /// Any <see cref="SynchronizationLockException"/> raised when releasing the lock is ignored.
        /// </remarks>
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

        /// <summary>
        /// Releases resources held by the ZoneTreeMessageStore and marks the instance as disposed.
        /// </summary>
        /// <remarks>
        /// Safe to call multiple times; after disposal, operations on the store will throw <see cref="ObjectDisposedException"/>.
        /// </remarks>
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
