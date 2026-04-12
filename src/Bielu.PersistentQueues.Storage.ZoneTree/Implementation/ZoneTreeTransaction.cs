using System;
using System.Collections.Generic;
using System.Threading;

namespace Bielu.PersistentQueues.Storage.ZoneTree;

/// <summary>
/// A transaction implementation for ZoneTree storage that buffers operations
/// and applies them on commit.
/// </summary>
/// <remarks>
/// ZoneTree does not support native multi-tree transactions, so this implementation
/// buffers operations and executes them sequentially on <see cref="Commit"/>.
/// If an operation fails mid-commit, earlier operations will have already been applied.
/// For most queue workloads (single-tree inserts/deletes) this is safe, but cross-tree
/// moves are handled with a write-first-then-delete strategy to prevent data loss.
/// </remarks>
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
    /// Applies all buffered operations sequentially.
    /// </summary>
    /// <remarks>
    /// ZoneTree does not support native multi-tree transactions. Operations are applied
    /// in order; if one fails, earlier operations remain applied. Cross-tree moves use
    /// a write-first-then-delete strategy so the worst case is a duplicate, not data loss.
    /// </remarks>
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

        if (_lock.IsWriteLockHeld)
        {
            _lock.ExitWriteLock();
        }
    }
}
