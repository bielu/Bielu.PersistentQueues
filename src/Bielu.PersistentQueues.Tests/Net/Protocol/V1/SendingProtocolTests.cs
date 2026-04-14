using System;
using System.Buffers;
using System.IO;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Bielu.PersistentQueues.Logging;
using Bielu.PersistentQueues.Network.Protocol.V1;
using Bielu.PersistentQueues.Network.Security;
using Bielu.PersistentQueues.Serialization;
using Bielu.PersistentQueues.Storage.LMDB;
using Shouldly;
using Xunit;
using Xunit.Abstractions;

namespace Bielu.PersistentQueues.Tests.Net.Protocol.V1;

public class SendingProtocolTests : TestBase
{
    public SendingProtocolTests(ITestOutputHelper output)
    {
        Output = output;
    }

    [Fact]
    public async Task writing_single_message()
    {
        var serializer = new MessageSerializer();
        using var env = LightningEnvironment();
        using var store = new LmdbMessageStore(env, serializer);
        var sender = new SendingProtocol(store, new NoSecurity(), serializer, new RecordingLogger(OutputWriter));
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(1));
        var expected = Message.Create(
            data: "hello"u8.ToArray(),
            queue: "test",
            destinationUri: "lq.tcp://fake:1234"
        );
        using var ms = new MemoryStream();
        //not exercising full protocol
        await Should.ThrowAsync<ProtocolViolationException>(async () =>
            await sender.SendAsync(new Uri("lq.tcp://localhost:5050"), ms, [expected], cancellation.Token)).ConfigureAwait(false);
        var bytes = new ReadOnlySequence<byte>(ms.ToArray());
        var msg = serializer.ToMessage(bytes.Slice(sizeof(int) * 2).FirstSpan);
        msg.Id.ShouldBe(expected.Id);
        await cancellation.CancelAsync().ConfigureAwait(false);
    }
}