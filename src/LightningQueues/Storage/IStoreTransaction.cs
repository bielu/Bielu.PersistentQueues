using System;

namespace LightningQueues.Storage;

/// <summary>
/// Represents a transaction for performing atomic storage operations.
/// </summary>
/// <remarks>
/// Storage providers implement this interface to provide transactional
/// semantics. Changes made within a transaction are either fully committed
/// or fully rolled back when the transaction is disposed without committing.
/// </remarks>
public interface IStoreTransaction : IDisposable
{
    /// <summary>
    /// Commits all changes made within this transaction.
    /// </summary>
    void Commit();
}
