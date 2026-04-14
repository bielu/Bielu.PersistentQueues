using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.IO.Pipelines;
using System.Linq;
using System.Net;
using System.Runtime.Serialization;
using System.Threading;
using System.Threading.Tasks;
using Bielu.PersistentQueues.Network.Security;
using Bielu.PersistentQueues.Serialization;
using Bielu.PersistentQueues.Storage;
using Microsoft.Extensions.Logging;

namespace Bielu.PersistentQueues.Network.Protocol.V1;

#pragma warning disable BIELU004 // ILogger is used intentionally to support both DI (ILogger<T>) and builder (ILogger) patterns
public class SendingProtocol(IMessageStore store, IStreamSecurity security, IMessageSerializer serializer, ILogger logger)
    : ProtocolBase(logger), ISendingProtocol
#pragma warning restore BIELU004
{
    private readonly IMessageStore _store = store;
    private readonly IStreamSecurity _security = security;
    private readonly IMessageSerializer _serializer = serializer;

    public async ValueTask SendAsync(Uri destination, Stream stream, List<Message> batch, CancellationToken token)
    {
        using var doneCancellation = new CancellationTokenSource();
        using var linkedCancel = CancellationTokenSource.CreateLinkedTokenSource(doneCancellation.Token, token);
        try
        {
            await SendImplAsync(destination, stream, batch, linkedCancel.Token).ConfigureAwait(false);
        }
        finally
        {
            await doneCancellation.CancelAsync().ConfigureAwait(false);
        }
    }

    private async ValueTask SendImplAsync(Uri destination, Stream stream, List<Message> messages, CancellationToken token)
    {
        stream = await _security.ApplyAsync(destination, stream).ConfigureAwait(false);
        
        var memory = _serializer.ToMemory(messages);

        var lengthBuffer = ArrayPool<byte>.Shared.Rent(sizeof(int));
        try
        {
            BinaryPrimitives.WriteInt32LittleEndian(lengthBuffer, memory.Length);
            await stream.WriteAsync(lengthBuffer.AsMemory(0, sizeof(int)), token).ConfigureAwait(false);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(lengthBuffer);
        }
        Logger.SenderWritingMessageBatch();
        await stream.WriteAsync(memory, token).ConfigureAwait(false);
        Logger.SenderSuccessfullyWroteMessageBatch();
        var pipe = new Pipe();
        var receiveTask = ReceiveIntoBufferAsync(pipe.Writer, stream, token);
        await ReadReceivedAsync(pipe.Reader, token).ConfigureAwait(false);
        Logger.SenderSuccessfullyReadReceived();
        var acknowledgeTask = WriteAcknowledgementAsync(stream, token);
        await Task.WhenAny(acknowledgeTask.AsTask(), receiveTask.AsTask()).ConfigureAwait(false);
        Logger.SenderSuccessfullyWroteAcknowledgement();
        _store.SuccessfullySent(messages);
        Logger.SenderStorageSuccessfullySent();
    }

    private static async ValueTask ReadReceivedAsync(PipeReader reader, CancellationToken token)
    {
        var result = await reader.ReadAtLeastAsync(Constants.ReceivedBuffer.Length, token).ConfigureAwait(false);
        var buffer = result.Buffer;
        if (SequenceEqual(ref buffer, Constants.ReceivedBuffer))
        {
            return;
        }
        if (SequenceEqual(ref buffer, Constants.SerializationFailureBuffer))
        {
            throw new SerializationException("The destination returned serialization error");
        }
        if (SequenceEqual(ref buffer, Constants.QueueDoesNotExistBuffer))
        {
            throw new QueueDoesNotExistException("Destination queue does not exist.");
        }

        throw new ProtocolViolationException("Unexpected outcome from send operation");
    }

    private static async ValueTask WriteAcknowledgementAsync(Stream stream, CancellationToken token)
    {
        await stream.WriteAsync(Constants.AcknowledgedMemory, token).ConfigureAwait(false);
    }

    public async ValueTask SendRawAsync(Uri destination, Stream stream, List<RawOutgoingMessage> rawMessages, CancellationToken token)
    {
        using var doneCancellation = new CancellationTokenSource();
        using var linkedCancel = CancellationTokenSource.CreateLinkedTokenSource(doneCancellation.Token, token);
        try
        {
            await SendRawImplAsync(destination, stream, rawMessages, linkedCancel.Token).ConfigureAwait(false);
        }
        finally
        {
            await doneCancellation.CancelAsync().ConfigureAwait(false);
        }
    }

    private async ValueTask SendRawImplAsync(Uri destination, Stream stream, List<RawOutgoingMessage> rawMessages, CancellationToken token)
    {
        stream = await _security.ApplyAsync(destination, stream).ConfigureAwait(false);

        // Calculate total payload size: 4-byte count + sum of all message sizes
        var totalSize = 4; // Message count prefix
        foreach (var msg in rawMessages)
        {
            totalSize += msg.FullMessage.Length;
        }

        // Write length prefix
        var lengthBuffer = ArrayPool<byte>.Shared.Rent(sizeof(int));
        try
        {
            BinaryPrimitives.WriteInt32LittleEndian(lengthBuffer, totalSize);
            await stream.WriteAsync(lengthBuffer.AsMemory(0, sizeof(int)), token).ConfigureAwait(false);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(lengthBuffer);
        }

        Logger.SenderWritingMessageBatch();

        // Write message count
        var countBuffer = ArrayPool<byte>.Shared.Rent(sizeof(int));
        try
        {
            BinaryPrimitives.WriteInt32LittleEndian(countBuffer, rawMessages.Count);
            await stream.WriteAsync(countBuffer.AsMemory(0, sizeof(int)), token).ConfigureAwait(false);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(countBuffer);
        }

        // Write each message's raw bytes directly (no re-serialization)
        foreach (var msg in rawMessages)
        {
            await stream.WriteAsync(msg.FullMessage, token).ConfigureAwait(false);
        }

        Logger.SenderSuccessfullyWroteMessageBatch();

        // Wait for acknowledgment
        var pipe = new Pipe();
        var receiveTask = ReceiveIntoBufferAsync(pipe.Writer, stream, token);
        await ReadReceivedAsync(pipe.Reader, token).ConfigureAwait(false);
        Logger.SenderSuccessfullyReadReceived();

        var acknowledgeTask = WriteAcknowledgementAsync(stream, token);
        await Task.WhenAny(acknowledgeTask.AsTask(), receiveTask.AsTask()).ConfigureAwait(false);
        Logger.SenderSuccessfullyWroteAcknowledgement();

        // Delete from storage using raw MessageIds
        _store.SuccessfullySentByIds(rawMessages.Select(m => m.MessageId));
        Logger.SenderStorageSuccessfullySent();
    }
}