using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Bielu.PersistentQueues.Network.Protocol;
using Microsoft.Extensions.Logging;

namespace Bielu.PersistentQueues.Network.Tcp;

public class Receiver : IDisposable
{
    private readonly TcpListener _listener;
    private readonly IReceivingProtocol _protocol;
    private readonly ILogger _logger;
    private bool _disposed;
        
    public Receiver(IPEndPoint endpoint, IReceivingProtocol protocol, ILogger logger)
    {
        Endpoint = endpoint;
        _protocol = protocol;
        _logger = logger;
        _listener = new TcpListener(Endpoint);
        _listener.Server.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
    }

    public IPEndPoint Endpoint { get; }

    public async ValueTask StartReceivingAsync(ChannelWriter<Message> receivedChannel, CancellationToken cancellationToken = default)
    {
        _listener.Start();
        try
        {
            while (!cancellationToken.IsCancellationRequested && !_disposed)
            {
                try
                {
                    var socket = await _listener.AcceptSocketAsync(cancellationToken).ConfigureAwait(false);
                    _ = HandleConnectionAsync(socket, receivedChannel, cancellationToken);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.ReceiverAcceptError(ex);
                }
            }
        }
        finally
        {
            _listener.Stop();
        }
    }

    private async Task HandleConnectionAsync(Socket socket, ChannelWriter<Message> receivedChannel, CancellationToken cancellationToken)
    {
        try
        {
            using (socket)
            await using (var stream = new NetworkStream(socket, false))
            {
                var messages = await _protocol.ReceiveMessagesAsync(stream, cancellationToken)
                    .ConfigureAwait(false);
                foreach (var msg in messages)
                {
                    await receivedChannel.WriteAsync(msg, cancellationToken).ConfigureAwait(false);
                }
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Expected during shutdown - don't log
        }
        catch (Exception ex)
        {
            try
            {
                _logger.ReceiverErrorReadingMessages(socket.RemoteEndPoint!, ex);
            }
            catch
            {
                // RemoteEndPoint may throw if socket is already disposed
            }
        }
    }

    public void Dispose()
    {
        if (_disposed)
            return;
        
        // Mark as disposed first to stop accepting new connections in StartReceivingAsync
        _disposed = true;
        _logger.ReceiverDisposing();
        
        try
        {
            // Safely stop the listener if it's running
            if(_listener.Server.IsBound)
                _listener.Stop();
        }
        catch (Exception ex)
        {
            // Just log and continue with disposal
            _logger.ReceiverDisposalError(ex);
        }
        
        GC.SuppressFinalize(this);
    }
}