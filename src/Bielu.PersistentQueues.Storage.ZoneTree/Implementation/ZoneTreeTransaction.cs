using System;
using System.Collections.Generic;

namespace Bielu.PersistentQueues.Storage.ZoneTree;

#pragma warning disable BIELU002 // Keep constructor internal so callers cannot fabricate provider transactions.

/// <summary>
/// Best-effort batching transaction for ZoneTree storage operations.
/// </summary>
/// <remarks>
/// ZoneTree's regular write APIs are already thread-safe. This type exists to satisfy
/// the IMessageStore transaction contract by delaying queued operations until Commit.
/// It does not provide storage-engine atomicity across multiple keys.
/// </remarks>
public class ZoneTreeTransaction : IStoreTransaction
{
    private readonly object _owner;
    private readonly Action<Action> _commitOperations;
    private readonly List<Action> _pendingOperations = new();
    private volatile bool _committed;
    private volatile bool _disposed;

    /// <summary>
    /// Creates a ZoneTree transaction batch owned by a specific message store.
    /// </summary>
    /// <param name="owner">The store instance that owns this transaction.</param>
    /// <param name="commitOperations">Callback used to execute queued operations under store lifecycle coordination.</param>
    internal ZoneTreeTransaction(object owner, Action<Action> commitOperations)
    {
        _owner = owner;
        _commitOperations = commitOperations;
    }

    /// <summary>
    /// Gets the owner store instance that created this transaction.
    /// </summary>
    internal object Owner => _owner;

    /// <summary>
    /// Buffers an operation to be executed when the transaction is committed.
    /// </summary>
    /// <param name="operation">The action to enqueue for execution during Commit.</param>
    internal void AddOperation(Action operation)
    {
        if (_committed)
            throw new InvalidOperationException("Cannot add operations to a committed transaction.");
        if (_disposed)
            throw new ObjectDisposedException(nameof(ZoneTreeTransaction));

        _pendingOperations.Add(operation);
    }

    /// <summary>
    /// Executes all buffered operations for the transaction.
    /// </summary>
    public void Commit()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(ZoneTreeTransaction));
        if (_committed)
            return;

        _commitOperations(() =>
        {
            while (_pendingOperations.Count > 0)
            {
                _pendingOperations[0]();
                _pendingOperations.RemoveAt(0);
            }
        });

        _committed = true;
    }

    /// <summary>
    /// Releases transaction resources and discards any uncommitted operations.
    /// </summary>
    public void Dispose()
    {
        GC.SuppressFinalize(this);
        if (_disposed)
            return;

        _disposed = true;
        _pendingOperations.Clear();
    }
}

#pragma warning restore BIELU002
