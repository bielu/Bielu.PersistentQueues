using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Bielu.PersistentQueues.Logging;
using Bielu.PersistentQueues.Network.Protocol;
using Bielu.PersistentQueues.Network.Protocol.V1;
using Bielu.PersistentQueues.Network.Security;
using Bielu.PersistentQueues.Storage;
using Bielu.PersistentQueues.Serialization;
using Bielu.PersistentQueues.Storage.LMDB;
using Shouldly;
using Xunit;
using Xunit.Abstractions;

namespace Bielu.PersistentQueues.Tests.Net.Protocol.V1;

public class ReceivingProtocolTests(ITestOutputHelper output) : TestBase
{

    [Fact]
    public async Task client_sending_negative_length_is_ignored()
    {
        await ReceivingScenarioAsync(async (protocol, _, token) =>
        {
            using var ms = new MemoryStream();
            ms.Write(BitConverter.GetBytes(-2), 0, 4);
            ms.Position = 0;
            var result = await protocol.ReceiveMessagesAsync(ms, token).ConfigureAwait(false);
        }).ConfigureAwait(false);
    }

    [Fact]
    public async Task handling_disconnects_mid_protocol_gracefully()
    {
        await ReceivingScenarioAsync(async (protocol, _, token) =>
        {
            using var ms = new MemoryStream();
            ms.Write(BitConverter.GetBytes(29));
            ms.Write("Fake this shouldn't pass!!!!!"u8);
            //even though we're not 'disconnecting', by making writable false it achieves the same outcome
            using var mockStream = new MemoryStream(ms.ToArray(), false);
            await Should.ThrowAsync<ProtocolViolationException>(async () =>
                await protocol.ReceiveMessagesAsync(mockStream, token).ConfigureAwait(false)).ConfigureAwait(false);
        }).ConfigureAwait(false);
    }

    [Fact]
    public async Task handling_valid_length()
    {
        await ReceivingScenarioAsync(async (protocol, logger, token) =>
        {
            await RunLengthTestAsync(protocol, 0, token).ConfigureAwait(false);
            logger.DebugMessages.Any(x => x.StartsWith("Received length")).ShouldBeTrue();
        }).ConfigureAwait(false);
    }

    [Fact]
    public async Task sending_shorter_length_than_payload_length()
    {
        await ReceivingScenarioAsync(async (protocol, _, token) =>
        {
            var ex = await Should.ThrowAsync<ProtocolViolationException>(async () =>
                await RunLengthTestAsync(protocol, -2, token).ConfigureAwait(false)).ConfigureAwait(false);
            ex.Message.ShouldBe("Protocol violation: received length of 70 bytes, but 72 bytes were available");
        }).ConfigureAwait(false);
    }

    [Fact]
    public async Task sending_longer_length_than_payload_length()
    {
        await ReceivingScenarioAsync(async (protocol, _, token) =>
        {
            var ex = await Should.ThrowAsync<ProtocolViolationException>(async () =>
                await RunLengthTestAsync(protocol, 5, token).ConfigureAwait(false)).ConfigureAwait(false);
            ex.Message.ShouldBe("Protocol violation: received length of 77 bytes, but 72 bytes were available");
        }).ConfigureAwait(false);
    }

    private async Task RunLengthTestAsync(IReceivingProtocol protocol, int differenceFromActualLength, CancellationToken token)
    {
        var message = Message.Create(
            data: "hello"u8.ToArray(),
            queue: "test"
        );
        var serializer = new MessageSerializer();
        var memory = serializer.ToMemory([message]);
        using var ms = new MemoryStream();
        ms.Write(BitConverter.GetBytes(memory.Length + differenceFromActualLength), 0, 4);
        ms.Write(memory.Span);
        ms.Position = 0;
        var msgs = await protocol.ReceiveMessagesAsync(ms, token).ConfigureAwait(false);
    }

    [Fact]
    public async Task sending_to_a_queue_that_doesnt_exist()
    {
        await ReceivingScenarioAsync(async (protocol, _, token) =>
        {
            var message = Message.Create(
                data: "hello"u8.ToArray(),
                queue: "test2"
            );
            var serializer = new MessageSerializer();
            var memory = serializer.ToMemory([message]);
            using var ms = new MemoryStream();
            ms.Write(BitConverter.GetBytes(memory.Length), 0, 4);
            ms.Write(memory.Span);
            ms.Position = 0;
            await Should.ThrowAsync<QueueDoesNotExistException>(async Task () =>
            {
                await protocol.ReceiveMessagesAsync(ms, token).ConfigureAwait(false);
            }).ConfigureAwait(false);
        }).ConfigureAwait(false);
    }

    [Fact]
    public async Task sending_data_that_is_cannot_be_deserialized()
    {
        await ReceivingScenarioAsync(async (protocol, _, token) =>
        {
            using var ms = new MemoryStream();
            ms.Write(BitConverter.GetBytes(215), 0, 4);
            ms.Write(Guid.NewGuid().ToByteArray(), 0, 16);
            ms.Write("sdlfjaslkdjfalsdjfalsjdfasjdflk;asjdfjdasdfaskldfjjflsjfklsjl"u8);
            ms.Write("sdlfjaslkdjfalsdjfalsjdfasjdflk;asjdfjdasdfaskldfjjflsjfklsjl"u8);
            ms.Write("sdlfjaslkdjfalsdjfalsjdfasjdflk;asjdfjdasdfaskldfjjflsjfklsjl"u8);
            ms.Write("blah"u8);
            ms.Write("yah"u8);
            ms.Write("tah"u8);
            ms.Write("fah"u8);
            ms.Write("wah"u8);
            ms.Position = 0;

            await Should.ThrowAsync<ProtocolViolationException>(async () =>
                await protocol.ReceiveMessagesAsync(ms, token).ConfigureAwait(false)).ConfigureAwait(false);
        }).ConfigureAwait(false);
    }

    [Fact]
    public async Task supports_ability_to_cancel_for_slow_clients()
    {
        await ReceivingScenarioAsync(async (protocol, logger, _) =>
        {
            var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));
            using var ms = new MemoryStream();
            var msgs = protocol.ReceiveMessagesAsync(ms, cts.Token);
            await DeterministicDelayAsync(TimeSpan.FromMilliseconds(200), CancellationToken.None).ConfigureAwait(false);
            ms.Write(BitConverter.GetBytes(5));
            cts.Token.IsCancellationRequested.ShouldBe(true);
            logger.DebugMessages.ShouldBeEmpty();
        }).ConfigureAwait(false);
    }

    private async Task ReceivingScenarioAsync(Func<ReceivingProtocol, RecordingLogger, CancellationToken, Task> scenario)
    {
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(1));
        var logger = new RecordingLogger(OutputWriter);
        var serializer = new MessageSerializer();
        using var env = LightningEnvironment();
        using var store = new LmdbMessageStore(env, serializer);
        store.CreateQueue("test");
        var protocol = new ReceivingProtocol(store, new NoSecurity(), serializer, new Uri("lq.tcp://localhost"), logger); 
        await scenario(protocol, logger, cancellation.Token).ConfigureAwait(false);
        await cancellation.CancelAsync().ConfigureAwait(false);
    }
}