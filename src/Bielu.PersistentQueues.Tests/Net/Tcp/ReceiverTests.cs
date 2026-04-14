using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Bielu.PersistentQueues.Logging;
using Bielu.PersistentQueues.Network.Protocol.V1;
using Bielu.PersistentQueues.Network.Security;
using Bielu.PersistentQueues.Network.Tcp;
using Bielu.PersistentQueues.Serialization;
using Bielu.PersistentQueues.Storage.LMDB;
using Shouldly;
using Xunit;
using Xunit.Abstractions;

namespace Bielu.PersistentQueues.Tests.Net.Tcp;

public class ReceiverTests(ITestOutputHelper output) : TestBase
{

    [Fact]
    public async Task stops_listening_on_task_cancellation()
    {
        await NetworkScenarioAsync(async (endpoint, _, _, token, receivingLoop, _) =>
        {
            var listener = new TcpListener(endpoint);
            Should.Throw<SocketException>(() => listener.Start());
            await token.CancelAsync().ConfigureAwait(false);
            await DeterministicDelayAsync(500, CancellationToken.None).ConfigureAwait(false);
            receivingLoop.IsCompleted.ShouldBe(true);
            listener.Start();
            listener.Stop();
        }).ConfigureAwait(false);
    }


    [Fact]
    public async Task can_handle_connect_then_disconnect()
    {
        await NetworkScenarioAsync(async (endpoint, _, _, token, receivingLoop, _) =>
        {
            using (var client = new TcpClient())
            {
                await client.ConnectAsync(endpoint.Address, endpoint.Port, token.Token).ConfigureAwait(false);
            }
            receivingLoop.IsFaulted.ShouldBeFalse();
        }).ConfigureAwait(false);
    }

    [Fact]
    public async Task can_handle_sending_three_bytes_then_disconnect()
    {
        await NetworkScenarioAsync(async (endpoint, _, _, token, receivingLoop, _) =>
        {
            using (var client = new TcpClient())
            {
                await client.ConnectAsync(endpoint.Address, endpoint.Port, token.Token).ConfigureAwait(false);
                await client.GetStream().WriteAsync((new byte[] { 1, 4, 6 }).AsMemory(0, 3), token.Token).ConfigureAwait(false);
            }
            receivingLoop.IsFaulted.ShouldBeFalse();
        }).ConfigureAwait(false);
    }

    [Fact]
    public async Task accepts_concurrently_connected_clients()
    {
        await NetworkScenarioAsync(async (endpoint, _, _, token, receivingTask, _) =>
        {
            using var client1 = new TcpClient();
            using var client2 = new TcpClient();
            await client1.ConnectAsync(endpoint.Address, endpoint.Port, token.Token).ConfigureAwait(false);
            await client2.ConnectAsync(endpoint.Address, endpoint.Port, token.Token).ConfigureAwait(false);
            await client2.GetStream()
                .WriteAsync(new byte[] { 1, 4, 6 }.AsMemory(0, 3), token.Token).ConfigureAwait(false);
            await client1.GetStream()
                .WriteAsync(new byte[] { 1, 4, 6 }.AsMemory(0, 3), token.Token).ConfigureAwait(false);
            receivingTask.IsFaulted.ShouldBeFalse();
        }).ConfigureAwait(false);
    }

    [Fact]
    public async Task receiving_a_valid_message()
    {
        var expected = NewMessage("test");
        await NetworkScenarioAsync(async (endpoint, sender, _, cancellation, _, channel) =>
        {
            var messages = new List<Message>([expected]);
            using var client = new TcpClient();
            await client.ConnectAsync(endpoint.Address, endpoint.Port, cancellation.Token).ConfigureAwait(false);
            await sender.SendAsync(expected.Destination!, client.GetStream(), messages, cancellation.Token).ConfigureAwait(false);

            var actual = await channel.Reader.ReadAsync(cancellation.Token).ConfigureAwait(false);
            await cancellation.CancelAsync().ConfigureAwait(false);
            actual.Id.ShouldBe(expected.Id);
            actual.QueueString.ShouldBe(expected.QueueString);
            Encoding.UTF8.GetString(actual.DataArray!).ShouldBe("hello");
        }, expected).ConfigureAwait(false);

    }

    private async Task NetworkScenarioAsync(Func<IPEndPoint, SendingProtocol, Receiver, CancellationTokenSource, Task, Channel<Message>, Task> scenario, Message? expected = null)
    {
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var endpoint = new IPEndPoint(IPAddress.Loopback, PortFinder.FindPort());
        var logger = new RecordingLogger(OutputWriter);
        var serializer = new MessageSerializer();
        using var env = LightningEnvironment();
        using var store = new LmdbMessageStore(env, serializer);
        store.CreateQueue("test");
        using var sendingEnv = LightningEnvironment();
        using var sendingStore = new LmdbMessageStore(sendingEnv, serializer);
        sendingStore.CreateQueue("test");
        
        if (expected.HasValue)
        {
            var expectedWithDestination = new Message(
                expected.Value.Id,
                expected.Value.Data,
                expected.Value.Queue,
                expected.Value.SentAt,
                expected.Value.SubQueue,
                $"lq.tcp://localhost:{endpoint.Port}".AsMemory(),
                expected.Value.DeliverBy,
                expected.Value.MaxAttempts,
                expected.Value.Headers
            );
            using var tx = sendingStore.BeginTransaction();
            sendingStore.StoreOutgoing(tx, expectedWithDestination);
            tx.Commit();
        }

        var sender = new SendingProtocol(sendingStore, new NoSecurity(), serializer, logger);
        var protocol = new ReceivingProtocol(store, new NoSecurity(), serializer, 
            new Uri($"lq.tcp://localhost:{endpoint.Port}"), logger);
        using var receiver = new Receiver(endpoint, protocol, logger); 
        var channel = Channel.CreateUnbounded<Message>();
        var receivingTask = Task.Factory.StartNew(() => 
            receiver.StartReceivingAsync(channel.Writer, cancellation.Token), cancellation.Token);
        await DeterministicDelayAsync(50, CancellationToken.None).ConfigureAwait(false);
        await scenario(endpoint, sender, receiver, cancellation, receivingTask, channel).ConfigureAwait(false);
        await cancellation.CancelAsync().ConfigureAwait(false);
    }
}