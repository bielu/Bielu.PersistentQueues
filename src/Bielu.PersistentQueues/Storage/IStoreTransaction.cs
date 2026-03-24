using System;

namespace Bielu.PersistentQueues.Storage;

/// <summary>
/// Represents a transaction for performing atomic storage operations.
/// </summary>
public interface IStoreTransaction : IDisposable
{
    /// <summary>
    /// Commits all changes made within this transaction.
    /// </summary>
    void Commit();
}
