using System;
using System.Buffers;
using System.IO;
using System.IO.Pipelines;
using System.Net.Sockets;
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

        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Expected during shutdown - don't log or rethrow
        }
        catch (IOException ex) when (IsRemoteDisconnect(ex))
        {
            // A peer can close/reset the TCP connection while the protocol reader
            // is still filling the pipe. Treat that the same as end-of-stream.
        }
        catch (Exception ex)
        {
            Logger.ProtocolStreamError(ex);
            throw;
        }
        finally
        {
            await writer.CompleteAsync().ConfigureAwait(false);
        }
    }

    private static bool IsRemoteDisconnect(IOException exception)
    {
        return exception.InnerException is SocketException
        {
            SocketErrorCode:
                SocketError.ConnectionReset or
                SocketError.ConnectionAborted or
                SocketError.Shutdown or
                SocketError.OperationAborted
        };
    }

    protected static bool SequenceEqual(ref ReadOnlySequence<byte> sequence, byte[] target)
    {
        var targetSpan = target.AsSpan();
        Span<byte> sequenceSpan = stackalloc byte[targetSpan.Length];
        sequence.CopyTo(sequenceSpan);
        return targetSpan.SequenceEqual(sequenceSpan);
    }
}
