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
    private readonly object _owner;
    private readonly List<Action> _pendingOperations = new();
    private volatile bool _committed;
    private volatile bool _disposed;

    /// <summary>
    /// Creates a ZoneTreeTransaction that will buffer operations and coordinate write access using the provided lock.
    /// </summary>
    /// <param name="transactionLock">The ReaderWriterLockSlim instance used to coordinate and release write access during the transaction's lifetime.</param>
    /// <param name="owner">The store instance that owns this transaction, used for ownership validation.</param>
    internal ZoneTreeTransaction(ReaderWriterLockSlim transactionLock, object owner)
    {
        _lock = transactionLock;
        _owner = owner;
    }

    /// <summary>
    /// Gets the owner store instance that created this transaction.
    /// </summary>
    internal object Owner => _owner;

    /// <summary>
    /// Buffers an operation to be executed when the transaction is committed.
    /// </summary>
    /// <param name="operation">The action to enqueue for execution during Commit.</param>
    /// <exception cref="InvalidOperationException">Thrown if the transaction has already been committed.</exception>
    /// <exception cref="ObjectDisposedException">Thrown if the transaction has been disposed.</exception>
    internal void AddOperation(Action operation)
    {
        if (_committed)
            throw new InvalidOperationException("Cannot add operations to a committed transaction.");
        if (_disposed)
            throw new ObjectDisposedException(nameof(ZoneTreeTransaction));
        _pendingOperations.Add(operation);
    }

    /// <summary>
    /// Executes all buffered operations for the transaction and marks the transaction as committed.
    /// </summary>
    /// <remarks>
    /// ZoneTree does not support native multi-tree transactions. Operations are applied
    /// in order; if one fails, earlier operations remain applied. Cross-tree moves use
    /// a write-first-then-delete strategy so the worst case is a duplicate, not data loss.
    /// On partial failure, successfully-applied operations are removed from the buffer so
    /// a retry will not replay them.
    /// </remarks>
    /// <exception cref="ObjectDisposedException">Thrown if the transaction has been disposed.</exception>
    public void Commit()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(ZoneTreeTransaction));
        if (_committed)
            return;

        // Process operations one at a time, removing each after successful execution
        // so that a retry after partial failure does not replay already-applied operations.
        while (_pendingOperations.Count > 0)
        {
            _pendingOperations[0]();
            _pendingOperations.RemoveAt(0);
        }

        _committed = true;
    }

    /// <summary>
    /// Releases transaction resources, clears buffered operations, and releases the associated write lock if held.
    /// </summary>
    /// <remarks>
    /// Suppresses finalization, marks the transaction as disposed, clears the pending operations buffer, and exits the write lock on the associated <see cref="ReaderWriterLockSlim"/> if it is currently held. Calling this method multiple times has no additional effect after the first call.
    /// </remarks>
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
