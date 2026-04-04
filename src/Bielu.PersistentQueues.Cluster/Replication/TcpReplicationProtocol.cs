using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Bielu.PersistentQueues.Storage;
using Microsoft.Extensions.Logging;

namespace Bielu.PersistentQueues.Cluster;

/// <summary>
/// TCP-based implementation of the replication protocol.
/// </summary>
/// <remarks>
/// <para>
/// On the sending side (primary), <see cref="ReplicateAsync"/> connects to the replica's
/// replication endpoint and sends a batch of messages as JSON over TCP. It waits for an
/// acknowledgment before returning.
/// </para>
/// <para>
/// On the receiving side (replica), <see cref="StartListeningAsync"/> accepts incoming
/// replication connections, stores the replicated messages in the local <see cref="IMessageStore"/>,
/// and sends back an acknowledgment.
/// </para>
/// </remarks>
public sealed class TcpReplicationProtocol : IReplicationProtocol
{
    private readonly IMessageStore _store;
    private readonly IPEndPoint _replicationEndpoint;
    private readonly ILogger _logger;
    private TcpListener? _listener;
    private CancellationTokenSource? _cts;
    private bool _disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="TcpReplicationProtocol"/> class.
    /// </summary>
    /// <param name="store">The local message store for persisting replicated messages.</param>
    /// <param name="replicationEndpoint">The local endpoint to listen on for replication.</param>
    /// <param name="logger">The logger instance.</param>
    public TcpReplicationProtocol(IMessageStore store, IPEndPoint replicationEndpoint, ILogger logger)
    {
        _store = store ?? throw new ArgumentNullException(nameof(store));
        _replicationEndpoint = replicationEndpoint ?? throw new ArgumentNullException(nameof(replicationEndpoint));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <inheritdoc />
    public async Task<bool> ReplicateAsync(
        IPEndPoint replicaEndpoint,
        string queueName,
        IReadOnlyList<Message> messages,
        long epoch,
        CancellationToken cancellationToken = default)
    {
        try
        {
            using var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            socket.NoDelay = true;

            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeoutCts.CancelAfter(TimeSpan.FromSeconds(10));

            await socket.ConnectAsync(replicaEndpoint, timeoutCts.Token).ConfigureAwait(false);
            await using var stream = new NetworkStream(socket, ownsSocket: false);

            // Serialize and send the replication request
            var request = new ReplicationRequest
            {
                Type = ReplicationMessageType.Replicate,
                QueueName = queueName,
                Epoch = epoch,
                Messages = messages.Select(m => new ReplicatedMessage
                {
                    SourceInstanceId = m.Id.SourceInstanceId,
                    MessageIdentifier = m.Id.MessageIdentifier,
                    Queue = m.Queue.ToString(),
                    Data = m.Data.ToArray(),
                    SentAt = m.SentAt,
                    SubQueue = m.SubQueue.ToString(),
                    PartitionKey = m.PartitionKey.ToString(),
                    DeliverBy = m.DeliverBy,
                    MaxAttempts = m.MaxAttempts
                }).ToList()
            };

            var json = JsonSerializer.Serialize(request);
            var requestBytes = Encoding.UTF8.GetBytes(json);

            // Write length prefix + data
            var lengthPrefix = BitConverter.GetBytes(requestBytes.Length);
            await stream.WriteAsync(lengthPrefix, timeoutCts.Token).ConfigureAwait(false);
            await stream.WriteAsync(requestBytes, timeoutCts.Token).ConfigureAwait(false);
            await stream.FlushAsync(timeoutCts.Token).ConfigureAwait(false);

            // Read acknowledgment
            var ackBuffer = new byte[1];
            var bytesRead = await stream.ReadAsync(ackBuffer, timeoutCts.Token).ConfigureAwait(false);

            if (bytesRead == 1 && ackBuffer[0] == 1)
            {
                _logger.LogDebug("Replication acknowledged by {Endpoint} for {QueueName} ({Count} messages)",
                    replicaEndpoint, queueName, messages.Count);
                return true;
            }

            _logger.LogWarning("Replication NAK from {Endpoint} for {QueueName}", replicaEndpoint, queueName);
            return false;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Replication to {Endpoint} failed for {QueueName}", replicaEndpoint, queueName);
            return false;
        }
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<Message>> SyncPartitionAsync(
        IPEndPoint sourceEndpoint,
        string queueName,
        MessageId? afterMessageId,
        CancellationToken cancellationToken = default)
    {
        try
        {
            using var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            socket.NoDelay = true;

            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeoutCts.CancelAfter(TimeSpan.FromSeconds(30));

            await socket.ConnectAsync(sourceEndpoint, timeoutCts.Token).ConfigureAwait(false);
            await using var stream = new NetworkStream(socket, ownsSocket: false);

            var request = new ReplicationRequest
            {
                Type = ReplicationMessageType.SyncPartition,
                QueueName = queueName,
                AfterSourceInstanceId = afterMessageId?.SourceInstanceId,
                AfterMessageIdentifier = afterMessageId?.MessageIdentifier
            };

            var json = JsonSerializer.Serialize(request);
            var requestBytes = Encoding.UTF8.GetBytes(json);
            var lengthPrefix = BitConverter.GetBytes(requestBytes.Length);
            await stream.WriteAsync(lengthPrefix, timeoutCts.Token).ConfigureAwait(false);
            await stream.WriteAsync(requestBytes, timeoutCts.Token).ConfigureAwait(false);
            await stream.FlushAsync(timeoutCts.Token).ConfigureAwait(false);

            // Read response length
            var responseLengthBuffer = new byte[4];
            await stream.ReadExactlyAsync(responseLengthBuffer, timeoutCts.Token).ConfigureAwait(false);
            var responseLength = BitConverter.ToInt32(responseLengthBuffer);

            // Read response body
            var responseBuffer = new byte[responseLength];
            await stream.ReadExactlyAsync(responseBuffer, timeoutCts.Token).ConfigureAwait(false);

            var responseJson = Encoding.UTF8.GetString(responseBuffer);
            var response = JsonSerializer.Deserialize<SyncResponse>(responseJson);

            if (response?.Messages == null)
                return Array.Empty<Message>();

            return response.Messages.Select(rm => Message.Create(
                id: rm.MessageIdentifier,
                data: rm.Data,
                queue: rm.Queue,
                partitionKey: rm.PartitionKey
            )).ToList().AsReadOnly();
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Sync partition from {Endpoint} failed for {QueueName}", sourceEndpoint, queueName);
            return Array.Empty<Message>();
        }
    }

    /// <inheritdoc />
    public async Task StartListeningAsync(CancellationToken cancellationToken = default)
    {
        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _listener = new TcpListener(_replicationEndpoint);
        _listener.Start();

        _logger.LogInformation("Replication listener started on {Endpoint}", _replicationEndpoint);

        while (!_cts.Token.IsCancellationRequested && !_disposed)
        {
            try
            {
                var socket = await _listener.AcceptSocketAsync(_cts.Token).ConfigureAwait(false);
                _ = HandleReplicationConnectionAsync(socket, _cts.Token);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error accepting replication connection");
            }
        }

        _listener.Stop();
    }

    private async Task HandleReplicationConnectionAsync(Socket socket, CancellationToken cancellationToken)
    {
        try
        {
            await using var stream = new NetworkStream(socket, ownsSocket: true);

            // Read length prefix
            var lengthBuffer = new byte[4];
            await stream.ReadExactlyAsync(lengthBuffer, cancellationToken).ConfigureAwait(false);
            var length = BitConverter.ToInt32(lengthBuffer);

            // Read request body
            var requestBuffer = new byte[length];
            await stream.ReadExactlyAsync(requestBuffer, cancellationToken).ConfigureAwait(false);

            var json = Encoding.UTF8.GetString(requestBuffer);
            var request = JsonSerializer.Deserialize<ReplicationRequest>(json);

            if (request == null) return;

            switch (request.Type)
            {
                case ReplicationMessageType.Replicate:
                    await HandleReplicateAsync(request, stream, cancellationToken).ConfigureAwait(false);
                    break;

                case ReplicationMessageType.SyncPartition:
                    await HandleSyncPartitionAsync(request, stream, cancellationToken).ConfigureAwait(false);
                    break;
            }
        }
        catch (OperationCanceledException)
        {
            // Expected during shutdown
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error handling replication connection");
        }
    }

    private async Task HandleReplicateAsync(ReplicationRequest request, NetworkStream stream, CancellationToken cancellationToken)
    {
        try
        {
            if (request.Messages == null || request.Messages.Count == 0)
            {
                await stream.WriteAsync(new byte[] { 0 }, cancellationToken).ConfigureAwait(false);
                return;
            }

            // Convert replicated messages to Message structs and store them
            var messages = request.Messages.Select(rm =>
            {
                var id = new MessageId
                {
                    SourceInstanceId = rm.SourceInstanceId,
                    MessageIdentifier = rm.MessageIdentifier
                };

                return new Message(
                    id,
                    rm.Data != null ? new ReadOnlyMemory<byte>(rm.Data) : ReadOnlyMemory<byte>.Empty,
                    rm.Queue?.AsMemory() ?? ReadOnlyMemory<char>.Empty,
                    rm.SentAt,
                    rm.SubQueue?.AsMemory() ?? ReadOnlyMemory<char>.Empty,
                    ReadOnlyMemory<char>.Empty, // DestinationUri not needed for replicated messages
                    rm.DeliverBy,
                    rm.MaxAttempts,
                    default, // FixedHeaders
                    partitionKey: rm.PartitionKey?.AsMemory() ?? ReadOnlyMemory<char>.Empty
                );
            }).ToList();

            _store.StoreIncoming(messages);

            // Send ACK
            await stream.WriteAsync(new byte[] { 1 }, cancellationToken).ConfigureAwait(false);
            _logger.LogDebug("Stored {Count} replicated messages for {QueueName}", messages.Count, request.QueueName);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error storing replicated messages for {QueueName}", request.QueueName);
            await stream.WriteAsync(new byte[] { 0 }, cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task HandleSyncPartitionAsync(ReplicationRequest request, NetworkStream stream, CancellationToken cancellationToken)
    {
        try
        {
            var queueName = request.QueueName ?? string.Empty;
            var allMessages = _store.PersistedIncoming(queueName).ToList();

            // If afterMessageId is specified, filter to messages after that ID
            // Since COMB GUIDs are chronologically sorted, we can use simple comparison
            List<ReplicatedMessage> replicatedMessages;
            if (request.AfterMessageIdentifier.HasValue)
            {
                var afterId = request.AfterMessageIdentifier.Value;
                var found = false;
                replicatedMessages = new List<ReplicatedMessage>();
                foreach (var msg in allMessages)
                {
                    if (found)
                    {
                        replicatedMessages.Add(ToReplicatedMessage(msg));
                    }
                    else if (msg.Id.MessageIdentifier == afterId)
                    {
                        found = true;
                    }
                }
            }
            else
            {
                replicatedMessages = allMessages.Select(ToReplicatedMessage).ToList();
            }

            var response = new SyncResponse { Messages = replicatedMessages };
            var responseJson = JsonSerializer.Serialize(response);
            var responseBytes = Encoding.UTF8.GetBytes(responseJson);
            var lengthPrefix = BitConverter.GetBytes(responseBytes.Length);

            await stream.WriteAsync(lengthPrefix, cancellationToken).ConfigureAwait(false);
            await stream.WriteAsync(responseBytes, cancellationToken).ConfigureAwait(false);
            await stream.FlushAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error handling sync partition request for {QueueName}", request.QueueName);
        }
    }

    private static ReplicatedMessage ToReplicatedMessage(Message m) => new()
    {
        SourceInstanceId = m.Id.SourceInstanceId,
        MessageIdentifier = m.Id.MessageIdentifier,
        Queue = m.Queue.ToString(),
        Data = m.Data.ToArray(),
        SentAt = m.SentAt,
        SubQueue = m.SubQueue.ToString(),
        PartitionKey = m.PartitionKey.ToString(),
        DeliverBy = m.DeliverBy,
        MaxAttempts = m.MaxAttempts
    };

    /// <inheritdoc />
    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _cts?.Cancel();
        _listener?.Stop();
        _cts?.Dispose();
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;
        if (_cts != null) await _cts.CancelAsync().ConfigureAwait(false);
        _listener?.Stop();
        _cts?.Dispose();
    }

    // ---- Wire format types ----

    private enum ReplicationMessageType
    {
        Replicate = 1,
        SyncPartition = 2
    }

    private sealed class ReplicationRequest
    {
        public ReplicationMessageType Type { get; set; }
        public string? QueueName { get; set; }
        public long Epoch { get; set; }
        public List<ReplicatedMessage>? Messages { get; set; }
        public Guid? AfterSourceInstanceId { get; set; }
        public Guid? AfterMessageIdentifier { get; set; }
    }

    private sealed class ReplicatedMessage
    {
        public Guid SourceInstanceId { get; set; }
        public Guid MessageIdentifier { get; set; }
        public string? Queue { get; set; }
        public byte[]? Data { get; set; }
        public DateTime SentAt { get; set; }
        public string? SubQueue { get; set; }
        public string? PartitionKey { get; set; }
        public DateTime? DeliverBy { get; set; }
        public int? MaxAttempts { get; set; }
    }

    private sealed class SyncResponse
    {
        public List<ReplicatedMessage>? Messages { get; set; }
    }
}
