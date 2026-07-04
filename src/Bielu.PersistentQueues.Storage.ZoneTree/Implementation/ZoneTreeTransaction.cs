using System;
using System.Collections.Generic;
using System.Threading;

namespace Bielu.PersistentQueues.Storage.ZoneTree;

/// <summary>
/// A transaction implementation for ZoneTree storage that buffers operations
/// and applies them on commit.
/// </summary>
/// <remarks>
/// ZoneTree's native transactions are scoped to a single tree, while this store uses one
/// tree per queue, so cross-queue operations like <c>MoveToQueue</c> could never be made
/// atomic by a native ZoneTree transaction anyway. This type is therefore an ordered,
/// best-effort batch, not an atomic transaction: operations are applied in order on
/// <see cref="Commit"/>, and if one fails, earlier operations remain applied. Cross-tree
/// moves use a write-first-then-delete strategy so the worst case on partial failure is a
/// duplicate message, never a lost one. On partial failure, successfully-applied operations
/// are removed from the buffer so a retry will not replay them.
/// </remarks>
/// <param name="storeLock">The store's <see cref="ReaderWriterLockSlim"/>, taken as a read lock during Commit so replay cannot race a queue being dropped.</param>
/// <param name="owner">The store instance that owns this transaction, used for ownership validation.</param>
public class ZoneTreeTransaction(ReaderWriterLockSlim storeLock, object owner) : IStoreTransaction
{
    private readonly List<Action> _pendingOperations = new();
    private volatile bool _committed;
    private volatile bool _disposed;

    /// <summary>
    /// Gets the owner store instance that created this transaction.
    /// </summary>
    internal object Owner => owner;

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
    /// Operations are applied in order under the store's read lock, so they run concurrently with
    /// other readers and writers but cannot race a queue being created or dropped. If one operation
    /// fails, earlier operations remain applied; successfully-applied operations are removed from the
    /// buffer so a retry will not replay them.
    /// </remarks>
    /// <exception cref="ObjectDisposedException">Thrown if the transaction has been disposed.</exception>
    public void Commit()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(ZoneTreeTransaction));
        if (_committed)
            return;

        storeLock.EnterReadLock();
        try
        {
            // Process operations one at a time, removing each after successful execution
            // so that a retry after partial failure does not replay already-applied operations.
            while (_pendingOperations.Count > 0)
            {
                _pendingOperations[0]();
                _pendingOperations.RemoveAt(0);
            }
        }
        finally
        {
            storeLock.ExitReadLock();
        }

        _committed = true;
    }

    /// <summary>
    /// Releases transaction resources and clears any buffered but uncommitted operations.
    /// </summary>
    /// <remarks>
    /// Suppresses finalization, marks the transaction as disposed, and clears the pending operations buffer.
    /// Calling this method multiple times has no additional effect after the first call.
    /// </remarks>
    public void Dispose()
    {
        GC.SuppressFinalize(this);
        if (_disposed)
            return;

        _disposed = true;
        _pendingOperations.Clear();
    }
}
