using System;
using System.Collections.Generic;
using System.Threading;

namespace Bielu.PersistentQueues.Storage.ZoneTree;

/// <summary>
/// A transaction implementation for ZoneTree storage that buffers operations
/// and applies them atomically on commit.
/// </summary>
public class ZoneTreeTransaction : IStoreTransaction
{
    private readonly ReaderWriterLockSlim _lock;
    private readonly List<Action> _pendingOperations = new();
    private bool _committed;
    private bool _disposed;

    internal ZoneTreeTransaction(ReaderWriterLockSlim transactionLock)
    {
        _lock = transactionLock;
    }

    /// <summary>
    /// Adds a pending operation to be executed on commit.
    /// </summary>
    internal void AddOperation(Action operation)
    {
        if (_committed)
            throw new InvalidOperationException("Cannot add operations to a committed transaction.");
        if (_disposed)
            throw new ObjectDisposedException(nameof(ZoneTreeTransaction));
        _pendingOperations.Add(operation);
    }

    /// <summary>
    /// Commits all buffered operations atomically.
    /// </summary>
    public void Commit()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(ZoneTreeTransaction));
        if (_committed)
            return;

        foreach (var operation in _pendingOperations)
        {
            operation();
        }

        _committed = true;
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);
        if (_disposed)
            return;

        _disposed = true;
        _pendingOperations.Clear();

        try
        {
            _lock.ExitWriteLock();
        }
        catch (SynchronizationLockException)
        {
            // Lock was already released
        }
    }
}
