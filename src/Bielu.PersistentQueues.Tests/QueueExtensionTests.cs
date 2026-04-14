using System.Linq;
using System.Threading.Tasks;
using Shouldly;
using Xunit;
using Xunit.Abstractions;

namespace Bielu.PersistentQueues.Tests;

public class QueueExtensionTests(ITestOutputHelper output) : TestBase(output)
{
    private record OrderMessage(string OrderId, decimal Amount, string Currency);

    [Fact]
    public async Task enqueue_strongly_typed_content()
    {
        await QueueScenarioAsync(async (queue, token) =>
        {
            var order = new OrderMessage("ORD-EXT-001", 55.00m, "USD");

            var receiveTask = queue.Receive("test", cancellationToken: token).FirstAsync(token);
            queue.Enqueue(order, queueName: "test");

            var result = await receiveTask;
            var deserialized = result.Message.GetContent<OrderMessage>();
            deserialized.ShouldNotBeNull();
            deserialized.OrderId.ShouldBe("ORD-EXT-001");
            deserialized.Amount.ShouldBe(55.00m);
            deserialized.Currency.ShouldBe("USD");
        });
    }

    [Fact]
    public async Task enqueue_strongly_typed_content_with_headers()
    {
        await QueueScenarioAsync(async (queue, token) =>
        {
            var order = new OrderMessage("ORD-EXT-002", 99.00m, "EUR");
            var headers = new System.Collections.Generic.Dictionary<string, string>
            {
                ["source"] = "api"
            };

            var receiveTask = queue.Receive("test", cancellationToken: token).FirstAsync(token);
            queue.Enqueue(order, queueName: "test", headers: headers);

            var result = await receiveTask;
            result.Message.GetHeadersDictionary()["source"].ShouldBe("api");
            result.Message.GetContent<OrderMessage>()!.OrderId.ShouldBe("ORD-EXT-002");
        });
    }

    [Fact]
    public async Task send_strongly_typed_content()
    {
        await QueueScenarioAsync(async (queue, token) =>
        {
            var order = new OrderMessage("ORD-SEND-001", 25.00m, "GBP");

            var receiveTask = queue.Receive("test", cancellationToken: token).FirstAsync(token);
            queue.Send(order,
                destinationUri: $"lq.tcp://localhost:{queue.Endpoint.Port}",
                queueName: "test");

            var result = await receiveTask;
            var deserialized = result.Message.GetContent<OrderMessage>();
            deserialized.ShouldNotBeNull();
            deserialized.OrderId.ShouldBe("ORD-SEND-001");
            deserialized.Amount.ShouldBe(25.00m);
        });
    }
}
