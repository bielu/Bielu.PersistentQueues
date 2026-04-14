using System;
using System.Buffers;
using System.IO;
using System.IO.Pipelines;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Bielu.PersistentQueues.Network.Protocol;

#pragma warning disable BIELU004 // ILogger is used intentionally to support both DI (ILogger<T>) and builder (ILogger) patterns
public abstract class ProtocolBase(ILogger logger)
#pragma warning restore BIELU004
{
    protected readonly ILogger Logger = logger;
    protected async ValueTask ReceiveIntoBufferAsync(PipeWriter writer, Stream stream, CancellationToken cancellationToken)
    {
        const int minimumBufferSize = 512;

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var memory = writer.GetMemory(minimumBufferSize);
                var bytesRead = await stream.ReadAsync(memory, cancellationToken).ConfigureAwait(false);
                if (bytesRead == 0)
                {
                    break;
                }

                writer.Advance(bytesRead);

                var result = await writer.FlushAsync(cancellationToken).ConfigureAwait(false);

                if (result.IsCompleted)
                {
                    break;
                }
            }

            await writer.CompleteAsync().ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Expected during shutdown - don't log or rethrow
        }
        catch (Exception ex)
        {
            Logger.ProtocolStreamError(ex);
            throw;
        }
    }

    protected static bool SequenceEqual(ref ReadOnlySequence<byte> sequence, byte[] target)
    {
        var targetSpan = target.AsSpan();
        Span<byte> sequenceSpan = stackalloc byte[targetSpan.Length];
        sequence.CopyTo(sequenceSpan);
        return targetSpan.SequenceEqual(sequenceSpan);
    }
}