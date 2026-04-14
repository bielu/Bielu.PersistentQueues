using System;
using System.Linq;
using System.Threading.Tasks;
using Bielu.PersistentQueues.Logging;
using Shouldly;
using Xunit;
using Xunit.Abstractions;

namespace Bielu.PersistentQueues.Tests;

public class IntegrationTests : TestBase
{
    public IntegrationTests(ITestOutputHelper output)
    {
        Output = output;
    }

    [Fact]
    public async Task can_send_and_receive_after_queue_not_found()
    {
        await QueueScenario(async (receiver, token) =>
        {
            var senderLogger = new RecordingLogger(OutputWriter);
            using var sender = new QueueConfiguration()
                .WithDefaultsForTest(Output)
                .LogWith(senderLogger)
                .BuildAndStartQueue("sender");
            var message1 = Message.Create(
                data: "hello"u8.ToArray(),
                queue: "receiver2",
                destinationUri: $"lq.tcp://localhost:{receiver.Endpoint.Port}"
            );
            sender.Send(message1);
            
            var message2 = Message.Create(
                data: "hello"u8.ToArray(),
                queue: "receiver",
                destinationUri: $"lq.tcp://localhost:{receiver.Endpoint.Port}"
            );
            sender.Send(message2);
            var received = await receiver.Receive("receiver", cancellationToken: token)
                .FirstAsync(token).ConfigureAwait(false);
            received.ShouldNotBeNull();
            received.Message.QueueString.ShouldBe(message2.QueueString);
            received.Message.DataArray.ShouldBe(message2.DataArray);
            
            await DeterministicDelay(100, token).ConfigureAwait(false);
            senderLogger.ErrorMessages.Any(x => x.Contains("Queue does not exist")).ShouldBeTrue();
        }, TimeSpan.FromSeconds(2), "receiver").ConfigureAwait(false);
    }
}