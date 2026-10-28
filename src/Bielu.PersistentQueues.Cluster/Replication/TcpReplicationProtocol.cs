using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Bielu.PersistentQueues.Partitioning;
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
    private static readonly TimeSpan ConnectionTimeout = TimeSpan.FromSeconds(30);
    private const int MaxSyncPageMessages = 1000;
    private const int SyncMessageOverheadBytes = 512;

    private readonly ILogger _logger;
    private readonly int _maxPayloadBytes;
    private TcpListener? _listener;
    private CancellationTokenSource? _cts;
    private bool _disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="TcpReplicationProtocol"/> class.
    /// </summary>
    /// <param name="store">The local message store for persisting replicated messages.</param>
    /// <param name="replicationEndpoint">The local endpoint to listen on for replication.</param>
    /// <param name="logger">The logger instance.</param>
    /// <param name="maxPayloadBytes">The maximum size of a single request or response frame.</param>
    /// <remarks>
    /// The listener does not authenticate peers and only accepts writes and reads for partition
    /// sub-queues. Expose the replication port only to other cluster nodes.
    /// </remarks>
    public TcpReplicationProtocol(IMessageStore store, IPEndPoint replicationEndpoint, ILogger<TcpReplicationProtocol> logger,
        int maxPayloadBytes = ClusterConfiguration.DefaultMaxPayloadBytes)
    {
        _store = store ?? throw new ArgumentNullException(nameof(store));
        _replicationEndpoint = replicationEndpoint ?? throw new ArgumentNullException(nameof(replicationEndpoint));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        if (maxPayloadBytes <= 0)
            throw new ArgumentOutOfRangeException(nameof(maxPayloadBytes), maxPayloadBytes, "Maximum payload size must be positive.");
        _maxPayloadBytes = maxPayloadBytes;
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
                Messages = messages.Select(ToReplicatedMessage).ToList()
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

            var responseBuffer = await ReadFrameAsync(stream, timeoutCts.Token).ConfigureAwait(false);
            var response = JsonSerializer.Deserialize<SyncResponse>(responseBuffer);

            if (response is not { Succeeded: true })
                throw new ClusterReplicationException(queueName, $"Source node {sourceEndpoint} could not read partition {queueName}.");

            if (response.Messages == null)
                return Array.Empty<Message>();

            return response.Messages.Select(FromReplicatedMessage).ToList().AsReadOnly();
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (ClusterReplicationException)
        {
            throw;
        }
        catch (Exception ex)
        {
            // An empty result would be indistinguishable from an empty partition, so report the failure.
            throw new ClusterReplicationException(queueName,
                $"Sync of partition {queueName} from {sourceEndpoint} failed: {ex.Message}", ex);
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
        // Bound the lifetime of every connection so idle or slow peers cannot pin handlers.
        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutCts.CancelAfter(ConnectionTimeout);
        var token = timeoutCts.Token;

        try
        {
            await using var stream = new NetworkStream(socket, ownsSocket: true);

            var requestBuffer = await ReadFrameAsync(stream, token).ConfigureAwait(false);
            var request = JsonSerializer.Deserialize<ReplicationRequest>(requestBuffer);

            if (request == null) return;

            switch (request.Type)
            {
                case ReplicationMessageType.Replicate:
                    await HandleReplicateAsync(request, stream, token).ConfigureAwait(false);
                    break;

                case ReplicationMessageType.SyncPartition:
                    await HandleSyncPartitionAsync(request, stream, token).ConfigureAwait(false);
                    break;
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Expected during shutdown
        }
        catch (OperationCanceledException)
        {
            _logger.LogWarning("Replication connection timed out after {Timeout}", ConnectionTimeout);
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

            // Only accept writes into the partition sub-queue named by the request, so a peer
            // cannot inject messages into arbitrary local queues.
            // Note: the epoch is carried on the wire but not enforced, because assignment epochs
            // are tracked per node and are not agreed on cluster-wide.
            if (!IsPartitionQueue(request.QueueName) ||
                request.Messages.Any(m => !string.Equals(m.Queue, request.QueueName, StringComparison.Ordinal)))
            {
                _logger.LogWarning("Rejected replication request for non-partition or mismatched queue {QueueName}",
                    request.QueueName);
                await stream.WriteAsync(new byte[] { 0 }, cancellationToken).ConfigureAwait(false);
                return;
            }

            // Convert replicated messages to Message structs and store them
            var messages = request.Messages.Select(FromReplicatedMessage).ToList();

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
        SyncResponse response;
        try
        {
            var queueName = request.QueueName ?? string.Empty;
            var afterMessageId = request.AfterMessageIdentifier.HasValue
                ? new MessageId
                {
                    SourceInstanceId = request.AfterSourceInstanceId ?? Guid.Empty,
                    MessageIdentifier = request.AfterMessageIdentifier.Value
                }
                : null;

            // Only partition sub-queues are replicated, so only those can be read back.
            var replicatedMessages = IsPartitionQueue(queueName)
                ? ReadSyncPage(queueName, afterMessageId)
                : [];

            response = new SyncResponse { Succeeded = true, Messages = replicatedMessages };
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error handling sync partition request for {QueueName}", request.QueueName);
            response = new SyncResponse { Succeeded = false };
        }

        var responseBytes = JsonSerializer.SerializeToUtf8Bytes(response);
        var lengthPrefix = BitConverter.GetBytes(responseBytes.Length);

        await stream.WriteAsync(lengthPrefix, cancellationToken).ConfigureAwait(false);
        await stream.WriteAsync(responseBytes, cancellationToken).ConfigureAwait(false);
        await stream.FlushAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Reads the next page of a partition, ordered by message ID so the requester can use the
    /// last ID as the cursor for the following page.
    /// </summary>
    private List<ReplicatedMessage> ReadSyncPage(string queueName, MessageId? afterMessageId)
    {
        // Leave room for JSON framing and the ~33% base64 expansion of the payloads.
        var byteBudget = _maxPayloadBytes / 2;
        var page = new List<ReplicatedMessage>();
        long pageBytes = 0;

        foreach (var message in _store.GetMessagesAfter(queueName, afterMessageId).OrderBy(m => m.Id.MessageIdentifier, MessageIdentifierComparer.Instance))
        {
            var size = EstimateSerializedSize(message);
            if (page.Count > 0 && (page.Count >= MaxSyncPageMessages || pageBytes + size > byteBudget))
                break;

            page.Add(ToReplicatedMessage(message));
            pageBytes += size;
        }

        return page;
    }

    private static long EstimateSerializedSize(Message message) =>
        SyncMessageOverheadBytes
        + message.Data.Length * 4L / 3
        + (message.Queue.Length + message.SubQueue.Length + message.PartitionKey.Length) * 2L
        + message.GetHeadersDictionary().Sum(h => (h.Key.Length + h.Value.Length) * 2L);

    private async Task<byte[]> ReadFrameAsync(NetworkStream stream, CancellationToken cancellationToken)
    {
        var lengthBuffer = new byte[4];
        await stream.ReadExactlyAsync(lengthBuffer, cancellationToken).ConfigureAwait(false);
        var length = BitConverter.ToInt32(lengthBuffer);

        if (length <= 0 || length > _maxPayloadBytes)
            throw new InvalidDataException($"Replication frame length {length} is outside the allowed range 1..{_maxPayloadBytes}.");

        var buffer = new byte[length];
        await stream.ReadExactlyAsync(buffer, cancellationToken).ConfigureAwait(false);
        return buffer;
    }

    private static bool IsPartitionQueue(string? queueName) =>
        queueName != null && PartitionConstants.TryParsePartitionQueueName(queueName, out _, out _);

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
        MaxAttempts = m.MaxAttempts,
        Headers = m.GetHeadersDictionary()
    };

    private static Message FromReplicatedMessage(ReplicatedMessage rm) => new(
        new MessageId
        {
            SourceInstanceId = rm.SourceInstanceId,
            MessageIdentifier = rm.MessageIdentifier
        },
        rm.Data != null ? new ReadOnlyMemory<byte>(rm.Data) : ReadOnlyMemory<byte>.Empty,
        rm.Queue?.AsMemory() ?? ReadOnlyMemory<char>.Empty,
        rm.SentAt,
        rm.SubQueue?.AsMemory() ?? ReadOnlyMemory<char>.Empty,
        ReadOnlyMemory<char>.Empty, // DestinationUri not needed for replicated messages
        rm.DeliverBy,
        rm.MaxAttempts,
        rm.Headers is { Count: > 0 } ? FixedHeaders.FromDictionary(rm.Headers) : default,
        partitionKey: rm.PartitionKey?.AsMemory() ?? ReadOnlyMemory<char>.Empty);

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
        public Dictionary<string, string>? Headers { get; set; }
    }

    private sealed class SyncResponse
    {
        public bool Succeeded { get; set; }
        public List<ReplicatedMessage>? Messages { get; set; }
    }

    /// <summary>
    /// Orders message identifiers by their byte representation, the same order
    /// <see cref="IMessageStore.GetMessagesAfter"/> uses for its cursor.
    /// </summary>
    private sealed class MessageIdentifierComparer : IComparer<Guid>
    {
        public static readonly MessageIdentifierComparer Instance = new();

        public int Compare(Guid x, Guid y)
        {
            Span<byte> left = stackalloc byte[16];
            Span<byte> right = stackalloc byte[16];
            x.TryWriteBytes(left);
            y.TryWriteBytes(right);
            return left.SequenceCompareTo(right);
        }
    }
}
