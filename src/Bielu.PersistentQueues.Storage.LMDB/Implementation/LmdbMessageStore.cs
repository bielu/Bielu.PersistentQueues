using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using LightningDB;
using Bielu.PersistentQueues.Serialization;

namespace Bielu.PersistentQueues.Storage.LMDB;

public class LmdbMessageStore : IMessageStore
{
    private const string OutgoingQueue = "outgoing";
    private readonly ReaderWriterLockSlim _lock;
    private readonly LightningEnvironment _environment;
    private readonly IMessageSerializer _serializer;
    private readonly ConcurrentDictionary<string, LightningDatabase> _cachedDatabases;
    private readonly IComparer<MDBValue>? _keyComparer;
    private readonly bool _useAppendData;
    private bool _disposed;

    public LmdbMessageStore(LightningEnvironment environment, IMessageSerializer serializer)
        : this(environment, serializer, null)
    {
    }

    public LmdbMessageStore(LightningEnvironment environment, IMessageSerializer serializer,
        LmdbStorageOptions? options)
    {
        _lock = new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion);
        _environment = environment;
        _serializer = serializer;
        _keyComparer = options?.KeyComparer;
        _useAppendData = options?.UseAppendData ?? false;
        _cachedDatabases = new ConcurrentDictionary<string, LightningDatabase>();
        if(!_environment.IsOpened)
        {
            var flags = options?.EnvironmentFlags ??
                (EnvironmentOpenFlags.NoLock | EnvironmentOpenFlags.MapAsync);
            _environment.Open(flags);
        }
        CreateQueue(OutgoingQueue);
    }
    
    public string Path => _environment.Path;

    /// <summary>
    /// Gets whether AppendData optimization is enabled for incoming message storage.
    /// </summary>
    public bool UseAppendData => _useAppendData;

    private static LightningTransaction GetLightningTransaction(IStoreTransaction transaction) =>
        transaction is LmdbTransaction lmdb
            ? lmdb.Transaction
            : throw new ArgumentException($"Expected LmdbTransaction but received {transaction.GetType().Name}", nameof(transaction));

    public void StoreIncoming(params IEnumerable<Message> messages)
    {
        CheckDisposed();
        
        _lock.EnterWriteLock();
        try
        {
            using var tx = _environment.BeginTransaction();
            StoreIncoming(tx, messages);
            ThrowIfError(tx.Commit());
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }
    
    private void CheckDisposed()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(LmdbMessageStore), "Cannot perform operation on a disposed message store");
    }

    public void StoreIncoming(IStoreTransaction transaction, params IEnumerable<Message> messages)
    {
        CheckDisposed();
        var tx = GetLightningTransaction(transaction);
        StoreIncoming(tx, messages);
    }

    private void StoreIncoming(LightningTransaction tx, params IEnumerable<Message> messages)
    {
        foreach (var messagesByQueue in messages.GroupBy(x => x.QueueString))
        {
            var queueName = messagesByQueue.Key!;
            var db = GetCachedDatabase(queueName);
            foreach (var message in messagesByQueue)
            {
                StoreIncomingMessage(tx, db, message);
            }
        }
    }

    private void StoreIncomingMessage(LightningTransaction tx, LightningDatabase db, Message message)
    {
        try
        {
            Span<byte> id = stackalloc byte[16];
            message.Id.MessageIdentifier.TryWriteBytes(id);
            ThrowIfError(tx.Put(db, id, _serializer.AsSpan(message)));
        }
        catch (StorageException ex)
        {
            if (ex.ResultCode == MDBResultCode.NotFound)
                throw new QueueDoesNotExistException(message.QueueString ?? "unknown", ex);
            throw;
        }
    }

    /// <summary>
    /// Zero-copy storage: stores raw wire-format bytes directly without re-serialization.
    /// This is significantly faster than StoreIncoming when bytes are already in wire format.
    /// </summary>
    /// <param name="messages">Pre-parsed message info from WireFormatSplitter</param>
    public void StoreRawIncoming(ReadOnlySpan<RawMessageInfo> messages)
    {
        CheckDisposed();

        _lock.EnterWriteLock();
        try
        {
            using var tx = _environment.BeginTransaction();
            StoreRawIncoming(tx, messages);
            ThrowIfError(tx.Commit());
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    private void StoreRawIncoming(LightningTransaction tx, ReadOnlySpan<RawMessageInfo> messages)
    {
        // Group by queue - use a simple approach since we can't use LINQ on Span
        // For typical small batches this is efficient
        foreach (var msg in messages)
        {
            var queueName = WireFormatSplitter.GetQueueName(in msg);
            var db = GetCachedDatabase(queueName);
            ThrowIfError(tx.Put(db, msg.MessageId.Span, msg.FullMessage.Span));
        }
    }

    /// <summary>
    /// Zero-copy storage implementing IMessageStore interface.
    /// Delegates to the optimized implementation, ignoring the serializer since
    /// we store raw bytes directly without deserialization.
    /// </summary>
    public void StoreRawIncoming(RawMessageInfo[] messages, int count, IMessageSerializer serializer)
    {
        // Delegate to optimized implementation - serializer not needed for zero-copy
        StoreRawIncoming(messages, count);
    }

    /// <summary>
    /// Zero-copy storage from array with offset and length.
    /// Optimized to avoid string allocation when all messages go to same queue.
    /// Uses AppendData optimization when configured in LmdbStorageOptions.
    /// </summary>
    public void StoreRawIncoming(RawMessageInfo[] messages, int count)
    {
        CheckDisposed();
        if (count == 0) return;

        _lock.EnterWriteLock();
        try
        {
            using var tx = _environment.BeginTransaction();
            var putOptions = _useAppendData ? PutOptions.AppendData : PutOptions.None;

            // Optimization: Check if all messages go to same queue (common case)
            // by comparing queue name bytes directly without string allocation
            var firstQueueBytes = messages[0].QueueNameBytes.Span;
            var allSameQueue = true;
            for (var i = 1; i < count && allSameQueue; i++)
            {
                allSameQueue = messages[i].QueueNameBytes.Span.SequenceEqual(firstQueueBytes);
            }

            if (allSameQueue)
            {
                // All messages go to same queue - single database lookup
                var queueName = Encoding.UTF8.GetString(firstQueueBytes);
                var db = GetCachedDatabase(queueName);
                for (var i = 0; i < count; i++)
                {
                    ThrowIfError(tx.Put(db, messages[i].MessageId.Span, messages[i].FullMessage.Span, putOptions));
                }
            }
            else
            {
                // Mixed queues - fall back to per-message lookup
                // Note: AppendData requires per-queue ascending order, which may not hold across queues
                for (var i = 0; i < count; i++)
                {
                    var msg = messages[i];
                    var queueName = Encoding.UTF8.GetString(msg.QueueNameBytes.Span);
                    var db = GetCachedDatabase(queueName);
                    ThrowIfError(tx.Put(db, msg.MessageId.Span, msg.FullMessage.Span, putOptions));
                }
            }
            ThrowIfError(tx.Commit());
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Ultra-fast zero-copy storage when queue name is known ahead of time.
    /// Eliminates string allocation, dictionary lookup, and queue name parsing.
    /// Uses AppendData optimization when configured in LmdbStorageOptions.
    /// </summary>
    public void StoreRawIncomingToQueue(string queueName, RawMessageInfo[] messages, int count)
    {
        CheckDisposed();
        if (count == 0) return;

        _lock.EnterWriteLock();
        try
        {
            var db = GetCachedDatabase(queueName);
            using var tx = _environment.BeginTransaction();
            var putOptions = _useAppendData ? PutOptions.AppendData : PutOptions.None;
            for (var i = 0; i < count; i++)
            {
                tx.Put(db, messages[i].MessageId.Span, messages[i].FullMessage.Span, putOptions);
            }
            tx.Commit();
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Maximum performance zero-copy storage with NO lock acquisition.
    /// ONLY use this when you guarantee single-threaded access (e.g., dedicated receiver thread).
    /// Provides the lowest possible latency by eliminating lock overhead.
    /// </summary>
    /// <remarks>
    /// WARNING: This method is NOT thread-safe. Using it from multiple threads concurrently
    /// will result in data corruption. Only use when you have exclusive access to the store.
    /// </remarks>
    public void StoreRawIncomingToQueueUnsafe(LightningDatabase db, RawMessageInfo[] messages, int count)
    {
        if (count == 0) return;

        using var tx = _environment.BeginTransaction();
        for (var i = 0; i < count; i++)
        {
            tx.Put(db, messages[i].MessageId.Span, messages[i].FullMessage.Span);
        }
        tx.Commit();
    }

    /// <summary>
    /// Maximum performance zero-copy storage with AppendData optimization.
    /// Requires keys to be inserted in strictly ascending order (COMB GUIDs with timestamp-first).
    /// Provides ~2x faster inserts by avoiding B+ tree rebalancing.
    /// </summary>
    /// <remarks>
    /// WARNING: This method is NOT thread-safe and requires strictly ascending keys.
    /// If keys are not in ascending order, LMDB will return an error.
    /// Use with fresh MessageId.GenerateRandom() keys which are timestamp-first COMBs.
    /// </remarks>
    public void StoreRawIncomingToQueueUnsafeAppend(LightningDatabase db, RawMessageInfo[] messages, int count)
    {
        if (count == 0) return;

        using var tx = _environment.BeginTransaction();
        for (var i = 0; i < count; i++)
        {
            tx.Put(db, messages[i].MessageId.Span, messages[i].FullMessage.Span, PutOptions.AppendData);
        }
        tx.Commit();
    }

    /// <summary>
    /// Gets a cached database handle for use with unsafe storage methods.
    /// The returned handle can be reused across multiple StoreRawIncomingToQueueUnsafe calls.
    /// </summary>
    public LightningDatabase GetDatabaseHandle(string queueName)
    {
        CheckDisposed();
        return GetCachedDatabase(queueName);
    }

    public void DeleteIncoming(IEnumerable<Message> messages)
    {
        CheckDisposed();
        
        _lock.EnterWriteLock();
        try
        {
            using var tx = _environment.BeginTransaction();
            foreach (var grouping in messages.GroupBy(x => x.QueueString))
            {
                RemoveMessagesFromStorage(tx, grouping.Key!, grouping);
            }
            ThrowIfError(tx.Commit());
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    private LightningDatabase GetCachedDatabase(string queueName)
    {
        if (_cachedDatabases.TryGetValue(queueName, out var cachedDb))
            return cachedDb;
        
        throw new QueueDoesNotExistException(queueName);
    }

    public IStoreTransaction BeginTransaction()
    {
        CheckDisposed();
        
        _lock.EnterWriteLock();
        try
        {
            // Note: When returning a transaction, caller is responsible for commit/rollback
            return new LmdbTransaction(_environment.BeginTransaction(), _lock);
        }
        catch
        {
            _lock.ExitWriteLock();
            throw;
        }
    }

    public void FailedToSend(bool shouldRemove = false, params IEnumerable<Message> messages)
    {
        CheckDisposed();
        
        _lock.EnterWriteLock();
        try
        {
            using var tx = _environment.BeginTransaction();
            foreach (var message in messages)
            {
                FailedToSend(tx, message);
            }
            ThrowIfError(tx.Commit());
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
            using var tx = _environment.BeginTransaction();
            SuccessfullySent(tx, messages);
            ThrowIfError(tx.Commit());
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    public void SuccessfullySentByIds(IEnumerable<ReadOnlyMemory<byte>> messageIds)
    {
        CheckDisposed();

        _lock.EnterWriteLock();
        try
        {
            var db = GetCachedDatabase(OutgoingQueue);
            using var tx = _environment.BeginTransaction();
            foreach (var messageId in messageIds)
            {
                var result = tx.Delete(db, messageId.Span);
                if (result != MDBResultCode.Success && result != MDBResultCode.NotFound)
                    throw new StorageException("Error with LightningDB delete operation", result);
            }
            ThrowIfError(tx.Commit());
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    public Message? GetMessage(string queueName, MessageId messageId)
    {
        CheckDisposed();
        Span<byte> id = stackalloc byte[16];
        messageId.MessageIdentifier.TryWriteBytes(id);
        
        _lock.EnterReadLock();
        try
        {
            using var tx = _environment.BeginTransaction(TransactionBeginFlags.ReadOnly);
            var db = GetCachedDatabase(queueName);
            var result = tx.Get(db, id);
            ThrowIfReadError(result.resultCode);
            if (result.resultCode == MDBResultCode.NotFound)
                return null;
            var messageBuffer = result.value.AsSpan();
            return _serializer.ToMessage(messageBuffer);
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    public string[] GetAllQueues()
    {
        CheckDisposed();
        // Filter out the outgoing queue
        return GetAllQueuesImpl()
            .Where(queueName => queueName != OutgoingQueue)
            .ToArray();
    }

    public void ClearAllStorage()
    {
        CheckDisposed();
        var queueNames = GetAllQueuesImpl();
        
        _lock.EnterWriteLock();
        try
        {
            using var tx = _environment.BeginTransaction();
            foreach (var queueName in queueNames)
            {
                var db = GetCachedDatabase(queueName);
                ThrowIfError(tx.TruncateDatabase(db));
            }

            ThrowIfError(tx.Commit());
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    private List<string> GetAllQueuesImpl()
    {
        // Pre-allocate with a reasonable size to avoid resizing
        var list = new List<string>(10);
        
        _lock.EnterReadLock();
        try
        {
            using var tx = _environment.BeginTransaction(TransactionBeginFlags.ReadOnly);
            using var db = tx.OpenDatabase();
            using var cursor = tx.CreateCursor(db);
            
            // LightningDB specific handling
            foreach (var (key, _) in cursor.AsEnumerable())
            {
                var keyBytes = key.AsSpan();
                var queueName = Encoding.UTF8.GetString(keyBytes);
                list.Add(queueName);
            }
        }
        finally
        {
            _lock.ExitReadLock();
        }
        return list;
    }

    private void SuccessfullySent(LightningTransaction tx, params IEnumerable<Message> messages)
    {
        RemoveMessagesFromStorage(tx, OutgoingQueue, messages);
    }

    // Streaming enumeration that yields messages one at a time for better memory efficiency
    private class MessageEnumerable : IEnumerable<Message>
    {
        private readonly LmdbMessageStore _store;
        private readonly string _queueName;

        public MessageEnumerable(LmdbMessageStore store, string queueName)
        {
            _store = store;
            _queueName = queueName;
        }

        public IEnumerator<Message> GetEnumerator()
        {
            return new MessageEnumerator(_store, _queueName);
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
    }

    private class MessageEnumerator : IEnumerator<Message>
    {
        private readonly LmdbMessageStore _store;
        private readonly string _queueName;
        private LightningTransaction? _transaction;
        private LightningDatabase? _database;
        private LightningCursor? _cursor;
        private IEnumerator<(LightningDB.MDBValue key, LightningDB.MDBValue value)>? _cursorEnumerator;
        private bool _disposed;

        public MessageEnumerator(LmdbMessageStore store, string queueName)
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
                _transaction = _store._environment.BeginTransaction(TransactionBeginFlags.ReadOnly);
                _database = _store.GetCachedDatabase(_queueName);
                _cursor = _transaction.CreateCursor(_database!);
                _cursorEnumerator = _cursor.AsEnumerable().GetEnumerator();
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
            if (_disposed || _cursorEnumerator == null)
                return false;

            try
            {
                if (_cursorEnumerator.MoveNext())
                {
                    var (_, value) = _cursorEnumerator.Current;
                    var valueSpan = value.AsSpan();
                    Current = _store._serializer.ToMessage(valueSpan);
                    return true;
                }
            }
            catch
            {
                // If there's an error, we should stop enumeration
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
                _cursorEnumerator?.Dispose();
                _cursor?.Dispose();
                _transaction?.Dispose();
            }
            finally
            {
                _store._lock.ExitReadLock();
                _cursorEnumerator = null;
                _cursor = null;
                _database = null;
                _transaction = null;
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

    private class RawOutgoingMessageEnumerable : IEnumerable<RawOutgoingMessage>
    {
        private readonly LmdbMessageStore _store;
        private readonly string _queueName;

        public RawOutgoingMessageEnumerable(LmdbMessageStore store, string queueName)
        {
            _store = store;
            _queueName = queueName;
        }

        public IEnumerator<RawOutgoingMessage> GetEnumerator() => new RawOutgoingMessageEnumerator(_store, _queueName);
        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
    }

    private class RawOutgoingMessageEnumerator : IEnumerator<RawOutgoingMessage>
    {
        private readonly LmdbMessageStore _store;
        private readonly string _queueName;
        private LightningTransaction? _transaction;
        private LightningDatabase? _database;
        private LightningCursor? _cursor;
        private IEnumerator<(LightningDB.MDBValue key, LightningDB.MDBValue value)>? _cursorEnumerator;
        private bool _disposed;

        public RawOutgoingMessageEnumerator(LmdbMessageStore store, string queueName)
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
                _transaction = _store._environment.BeginTransaction(TransactionBeginFlags.ReadOnly);
                _database = _store.GetCachedDatabase(_queueName);
                _cursor = _transaction.CreateCursor(_database!);
                _cursorEnumerator = _cursor.AsEnumerable().GetEnumerator();
            }
            catch
            {
                Cleanup();
                throw;
            }
        }

        public RawOutgoingMessage Current { get; private set; }

        object System.Collections.IEnumerator.Current => Current;

        public bool MoveNext()
        {
            if (_disposed || _cursorEnumerator == null)
                return false;

            try
            {
                if (_cursorEnumerator.MoveNext())
                {
                    var (_, value) = _cursorEnumerator.Current;
                    // Copy bytes to array since MDBValue is only valid during enumeration step
                    var valueArray = value.AsSpan().ToArray();
                    Current = WireFormatReader.ReadOutgoingMessage(valueArray, 0, valueArray.Length);
                    return true;
                }
            }
            catch
            {
                // If there's an error, we should stop enumeration
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
                _cursorEnumerator?.Dispose();
                _cursor?.Dispose();
                _transaction?.Dispose();
            }
            finally
            {
                _store._lock.ExitReadLock();
                _cursorEnumerator = null;
                _cursor = null;
                _database = null;
                _transaction = null;
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

    public IEnumerable<Message> PersistedIncoming(string queueName)
    {
        CheckDisposed();
        return new MessageEnumerable(this, queueName);
    }

    public IEnumerable<Message> PersistedOutgoing()
    {
        CheckDisposed();
        return new MessageEnumerable(this, OutgoingQueue);
    }

    public IEnumerable<RawOutgoingMessage> PersistedOutgoingRaw()
    {
        CheckDisposed();
        return new RawOutgoingMessageEnumerable(this, OutgoingQueue);
    }

    public void MoveToQueue(IStoreTransaction transaction, string queueName, Message message)
    {
        CheckDisposed();
        var tx = GetLightningTransaction(transaction);
        MoveToQueue(tx, queueName, message);
    }

    public void MoveToQueue(IStoreTransaction transaction, string queueName, IEnumerable<Message> messages)
    {
        CheckDisposed();
        var tx = GetLightningTransaction(transaction);
        foreach (var message in messages)
        {
            MoveToQueue(tx, queueName, message);
        }
    }

    public void SuccessfullyReceived(IStoreTransaction transaction, Message message)
    {
        CheckDisposed();
        var tx = GetLightningTransaction(transaction);
        SuccessfullyReceived(tx, message);
    }

    public void SuccessfullyReceived(IStoreTransaction transaction, IEnumerable<Message> messages)
    {
        CheckDisposed();
        var tx = GetLightningTransaction(transaction);
        foreach (var message in messages)
        {
            SuccessfullyReceived(tx, message);
        }
    }

    private void SuccessfullyReceived(LightningTransaction tx, Message message)
    {
        var db = GetCachedDatabase(message.QueueString!);
        RemoveMessageFromStorage(tx, db, message);
    }

    private void RemoveMessagesFromStorage(LightningTransaction tx, string queueName, params IEnumerable<Message> messages)
    {
        var db = GetCachedDatabase(queueName);
        foreach (var message in messages)
        {
            RemoveMessageFromStorage(tx, db, message);
        }
    }

    private static void RemoveMessageFromStorage(LightningTransaction tx, LightningDatabase db, Message message)
    {
        Span<byte> id = stackalloc byte[16];
        message.Id.MessageIdentifier.TryWriteBytes(id);
        var result = tx.Delete(db, id);
        if (result != MDBResultCode.Success && result != MDBResultCode.NotFound)
            throw new StorageException("Error with LightningDB operation", result);
    }

    public void StoreOutgoing(IStoreTransaction transaction, Message message)
    {
        CheckDisposed();
        var tx = GetLightningTransaction(transaction);
        var db = GetCachedDatabase(OutgoingQueue);
        StoreOutgoing(tx, db, message);
    }

    public void StoreOutgoing(Message message)
    {
        CheckDisposed();
        
        _lock.EnterWriteLock();
        try
        {
            using var tx = _environment.BeginTransaction();
            var db = GetCachedDatabase(OutgoingQueue);
            StoreOutgoing(tx, db, message);
            tx.Commit();
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    public void StoreOutgoing(IEnumerable<Message> messages)
    {
        CheckDisposed();
        
        _lock.EnterWriteLock();
        try
        {
            using var tx = _environment.BeginTransaction();
            var db = GetCachedDatabase(OutgoingQueue);
            foreach (var message in messages)
            {
                StoreOutgoing(tx, db, message);
            }
            tx.Commit();
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
            using var tx = _environment.BeginTransaction();
            var db = GetCachedDatabase(OutgoingQueue);
            foreach (var message in messages)
            {
                StoreOutgoing(tx, db, message);
            }
            tx.Commit();
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    private void StoreOutgoing(LightningTransaction tx, LightningDatabase db, Message message)
    {
        Span<byte> id = stackalloc byte[16];
        message.Id.MessageIdentifier.TryWriteBytes(id);
        ThrowIfError(tx.Put(db, id, _serializer.AsSpan(message)));
    }

    private static void ThrowIfError(MDBResultCode resultCode)
    {
        if (resultCode != MDBResultCode.Success)
            throw new StorageException("Error with LightningDB operation", resultCode);
    }

    private static void ThrowIfReadError(MDBResultCode resultCode)
    {
        if (resultCode != MDBResultCode.Success && resultCode != MDBResultCode.NotFound)
            throw new StorageException("Error with LightningDB read operation", resultCode);
    }

    private void FailedToSend(LightningTransaction tx, Message message)
    {
        Span<byte> id = stackalloc byte[16];
        message.Id.MessageIdentifier.TryWriteBytes(id);
        var db = GetCachedDatabase(OutgoingQueue);
        var value = tx.Get(db, id);
        if (value.resultCode == MDBResultCode.NotFound)
            return;
        var valueBuffer = value.value.AsSpan();
        var msg = _serializer.ToMessage(valueBuffer);
        var attempts = message.SentAttempts;
        if (attempts >= message.MaxAttempts)
        {
            RemoveMessageFromStorage(tx, db, msg);
        }
        else if (msg.DeliverBy.HasValue)
        {
            var expire = msg.DeliverBy.Value;
            if (expire != DateTime.MinValue && DateTime.Now >= expire)
            {
                RemoveMessageFromStorage(tx, db, msg);
            }
        }
        else
        {
            ThrowIfError(tx.Put(db, id, _serializer.AsSpan(msg)));
        }
    }

    private void MoveToQueue(LightningTransaction tx, string queueName, Message message)
    {
        try
        {
            Span<byte> id = stackalloc byte[16];
            message.Id.MessageIdentifier.TryWriteBytes(id);
            var original = GetCachedDatabase(message.QueueString!);
            var newDb = GetCachedDatabase(queueName);
            ThrowIfError(tx.Delete(original, id));
            
            // Create updated message with new queue name for storage
            var updatedMessage = new Message(
                message.Id,
                message.Data,
                queueName.AsMemory(),
                message.SentAt,
                message.SubQueue,
                message.DestinationUri,
                message.DeliverBy,
                message.MaxAttempts,
                message.Headers
            );
            ThrowIfError(tx.Put(newDb, id, _serializer.AsSpan(updatedMessage)));
        }
        catch (LightningException ex)
        {
            tx.Dispose();
            if (ex.StatusCode == (int)MDBResultCode.NotFound)
                throw new QueueDoesNotExistException(queueName, ex);
            throw;
        }
    }

    public void CreateQueue(string queueName)
    {
        CheckDisposed();

        _lock.EnterWriteLock();
        try
        {
            using var tx = _environment.BeginTransaction();
            var config = new DatabaseConfiguration { Flags = DatabaseOpenFlags.Create };
            if (_keyComparer != null)
                config.CompareWith(_keyComparer);
            var db = tx.OpenDatabase(queueName, config);
            ThrowIfError(tx.Commit());

            // Cache the database handle
            _cachedDatabases.TryAdd(queueName, db);
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    /// <inheritdoc />
    public StorageUsageInfo? GetStorageUsageInfo()
    {
        CheckDisposed();

        var info = _environment.Info;
        var stats = _environment.EnvironmentStats;

        var totalBytes = info.MapSize;
        // LastPageNumber is 0-based, so used pages = LastPageNumber + 1
        var usedBytes = (info.LastPageNumber + 1) * stats.PageSize;

        // Calculate free bytes from the environment stats
        long freeBytes = 0;
        try
        {
            // In LMDB, the free list is stored in a special database with ID 0.
            // LightningDB EnvironmentStats mapping for free pages is ms_free_pages.
            // We iterate all members of EnvironmentStats to find anything related to free pages.
            var type = stats.GetType();
            var members = type.GetMembers(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            
            foreach (var member in members)
            {
                if (member.Name.Contains("free", StringComparison.OrdinalIgnoreCase) && member.Name.Contains("page", StringComparison.OrdinalIgnoreCase))
                {
                    object? value = null;
                    if (member is System.Reflection.PropertyInfo p) value = p.GetValue(stats);
                    else if (member is System.Reflection.FieldInfo f) value = f.GetValue(stats);
                    
                    if (value != null)
                    {
                        freeBytes = Convert.ToInt64(value) * stats.PageSize;
                        break;
                    }
                }
            }
        }
        catch
        {
            // Fallback to 0
        }

        return new StorageUsageInfo(usedBytes, totalBytes, freeBytes);
    }

    /// <summary>
    /// Gets the number of persisted incoming messages for a specified queue using
    /// LMDB's O(1) database statistics instead of enumerating all messages.
    /// </summary>
    /// <param name="queueName">The name of the queue to count messages in.</param>
    /// <returns>The number of messages in the queue.</returns>
    public long GetMessageCount(string queueName)
    {
        CheckDisposed();

        _lock.EnterReadLock();
        try
        {
            var db = GetCachedDatabase(queueName);
            using var tx = _environment.BeginTransaction(TransactionBeginFlags.ReadOnly);
            return tx.GetEntriesCount(db);
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }



    public void Dispose()
    {
        GC.SuppressFinalize(this);
        Dispose(true);
    }

    ~LmdbMessageStore()
    {
        Dispose(false);
    }

    private void Dispose(bool disposing)
    {
        if (_disposed)
            return;
            
        // First mark as disposed to prevent new operations
        _disposed = true;
        
        if (disposing)
        {
            // Use write lock to ensure we don't dispose while another operation is in progress
            _lock.EnterWriteLock();
            try
            {
                try
                {
                    _cachedDatabases.Clear();
                    
                    //Flush due to async setup for more optimal performance
                    _environment.Flush(true);
                    // Dispose the environment in a controlled manner
                    _environment.Dispose();
                }
                catch (Exception)
                {
                    // Swallow exceptions during disposal to prevent disruption
                    // We're already tearing down, so not much we can do about failures
                }
            }
            finally
            {
                _lock.ExitWriteLock();
                
                // Dispose the lock itself
                _lock.Dispose();
            }
        }
    }
}