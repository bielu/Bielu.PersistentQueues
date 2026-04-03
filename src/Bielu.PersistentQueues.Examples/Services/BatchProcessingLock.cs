using System;
using System.Threading;
using System.Threading.Tasks;

namespace Bielu.PersistentQueues.Examples.Services;

/// <summary>
/// Shared exclusive lock that ensures only one consumer (partition worker or
/// priority-alert consumer) is actively processing a batch at any given moment.
/// </summary>
internal sealed class BatchProcessingLock : IDisposable
{
    private readonly SemaphoreSlim _semaphore = new(1, 1);

    public Task WaitAsync(CancellationToken cancellationToken)
        => _semaphore.WaitAsync(cancellationToken);

    public void Release() => _semaphore.Release();

    public void Dispose() => _semaphore.Dispose();
}
