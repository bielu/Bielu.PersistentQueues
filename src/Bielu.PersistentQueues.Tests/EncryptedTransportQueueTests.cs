using System;
using System.Linq;
using System.Threading.Tasks;
using Shouldly;
using Xunit;
using Xunit.Abstractions;

namespace Bielu.PersistentQueues.Tests;

public class EncryptedTransportQueueTests(ITestOutputHelper output) : TestBase
{

    [Fact]
    public async Task can_send_and_receive_messages_over_TLS1_2()
    {
        await QueueScenarioAsync(config =>
        {
            config.WithSelfSignedCertificateSecurity();
        }, async (queue, token) =>
        {
            var message = Message.Create(
                data: "hello"u8.ToArray(),
                queue: "test",
                destinationUri: $"lq.tcp://localhost:{queue.Endpoint.Port}"
            );
            await DeterministicDelayAsync(100, token).ConfigureAwait(false);
            queue.Send(message);
            var received = await queue.Receive("test", cancellationToken: token)
                .FirstAsync(token).ConfigureAwait(false);
            received.ShouldNotBeNull();
            received.Message.QueueString.ShouldBe(message.QueueString);
            received.Message.DataArray.ShouldBe(message.DataArray);
        }, TimeSpan.FromSeconds(5)).ConfigureAwait(false);
    }
}