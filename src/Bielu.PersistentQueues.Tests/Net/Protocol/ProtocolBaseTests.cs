using System;
using System.IO;
using System.IO.Pipelines;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Bielu.PersistentQueues.Logging;
using Bielu.PersistentQueues.Network.Protocol;
using Shouldly;
using Xunit;

namespace Bielu.PersistentQueues.Tests.Net.Protocol;

public class ProtocolBaseTests
{
    [Fact]
    public async Task remote_socket_reset_completes_pipe_without_logging_error()
    {
        var logger = new RecordingLogger();
        var protocol = new TestProtocol(logger);
        var pipe = new Pipe();

        await protocol.ReceiveAsync(pipe.Writer, new ResettingStream(), CancellationToken.None);

        var result = await pipe.Reader.ReadAsync();
        result.IsCompleted.ShouldBeTrue();
        result.Buffer.IsEmpty.ShouldBeTrue();
        logger.ErrorMessages.ShouldBeEmpty();
    }

    private sealed class TestProtocol(RecordingLogger logger) : ProtocolBase(logger)
    {
        public ValueTask ReceiveAsync(PipeWriter writer, Stream stream, CancellationToken cancellationToken)
        {
            return ReceiveIntoBufferAsync(writer, stream, cancellationToken);
        }
    }

    private sealed class ResettingStream : Stream
    {
        public override bool CanRead => true;

        public override bool CanSeek => false;

        public override bool CanWrite => false;

        public override long Length => throw new NotSupportedException();

        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        public override void Flush()
        {
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            throw CreateException();
        }

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            return ValueTask.FromException<int>(CreateException());
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotSupportedException();
        }

        public override void SetLength(long value)
        {
            throw new NotSupportedException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException();
        }

        private static IOException CreateException()
        {
            return new IOException(
                "Unable to read data from the transport connection.",
                new SocketException((int)SocketError.ConnectionReset));
        }
    }
}
