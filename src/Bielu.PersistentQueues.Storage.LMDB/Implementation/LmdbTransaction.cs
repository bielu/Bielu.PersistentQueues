using System;
using System.Threading;
using LightningDB;

namespace Bielu.PersistentQueues.Storage.LMDB;

#pragma warning disable BIELU010 // LmdbTransaction is an IStoreTransaction adapter, not a LightningTransaction wrapper
public class LmdbTransaction(LightningTransaction tx, ReaderWriterLockSlim transactionLock)
    : IStoreTransaction
#pragma warning restore BIELU010
{
    public LightningTransaction Transaction { get; } = tx;

    public void Commit()
    {
        if (!Transaction.Environment.IsOpened)
            return;
        Transaction.Commit().ThrowOnError();
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);
        try
        {
            Transaction.Dispose();
        }
        finally
        {
            transactionLock.ExitWriteLock();
        }
    }
}