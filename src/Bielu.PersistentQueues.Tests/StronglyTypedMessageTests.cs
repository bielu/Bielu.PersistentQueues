using System;
using System.Collections.Generic;
using System.Text.Json;
using Shouldly;
using Xunit;

namespace Bielu.PersistentQueues.Tests;

public class StronglyTypedMessageTests
{
    private record OrderMessage(string OrderId, decimal Amount, string Currency);

    private record CustomerMessage(int Id, string Name, string Email);

    [Fact]
    public void create_with_strongly_typed_content_and_get_content()
    {
        var order = new OrderMessage("ORD-123", 99.95m, "USD");

        var message = Message.Create(order, queue: "orders");

        var deserialized = message.GetContent<OrderMessage>();
        deserialized.ShouldNotBeNull();
        deserialized.OrderId.ShouldBe("ORD-123");
        deserialized.Amount.ShouldBe(99.95m);
        deserialized.Currency.ShouldBe("USD");
    }

    [Fact]
    public void create_with_strongly_typed_content_preserves_queue_metadata()
    {
        var customer = new CustomerMessage(42, "John Doe", "john@example.com");

        var message = Message.Create(
            customer,
            queue: "customers",
            subQueue: "vip",
            partitionKey: "region-1"
        );

        message.QueueString.ShouldBe("customers");
        message.SubQueueString.ShouldBe("vip");
        message.PartitionKeyString.ShouldBe("region-1");

        var deserialized = message.GetContent<CustomerMessage>();
        deserialized.ShouldNotBeNull();
        deserialized.Id.ShouldBe(42);
        deserialized.Name.ShouldBe("John Doe");
        deserialized.Email.ShouldBe("john@example.com");
    }

    [Fact]
    public void create_with_strongly_typed_content_preserves_headers()
    {
        var order = new OrderMessage("ORD-456", 50.00m, "EUR");
        var headers = new Dictionary<string, string> { ["source"] = "web" };

        var message = Message.Create(order, queue: "orders", headers: headers);

        message.GetHeadersDictionary()["source"].ShouldBe("web");
        message.GetContent<OrderMessage>()!.OrderId.ShouldBe("ORD-456");
    }

    [Fact]
    public void create_with_strongly_typed_content_and_custom_serializer_options()
    {
        var order = new OrderMessage("ORD-789", 123.45m, "GBP");
        var options = new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };

        var message = Message.Create(order, jsonSerializerOptions: options, queue: "orders");

        var deserialized = message.GetContent<OrderMessage>(options);
        deserialized.ShouldNotBeNull();
        deserialized.OrderId.ShouldBe("ORD-789");
        deserialized.Amount.ShouldBe(123.45m);
    }

    [Fact]
    public void create_with_strongly_typed_content_generates_id_when_not_provided()
    {
        var order = new OrderMessage("ORD-001", 10.00m, "USD");

        var message = Message.Create(order, queue: "test");

        message.Id.MessageIdentifier.ShouldNotBe(Guid.Empty);
    }

    [Fact]
    public void create_with_strongly_typed_content_uses_provided_id()
    {
        var expectedId = Guid.NewGuid();
        var order = new OrderMessage("ORD-002", 20.00m, "USD");

        var message = Message.Create(order, id: expectedId, queue: "test");

        message.Id.MessageIdentifier.ShouldBe(expectedId);
    }

    [Fact]
    public void get_content_returns_default_for_empty_data()
    {
        var message = Message.Create(queue: "test");

        var result = message.GetContent<OrderMessage>();

        result.ShouldBeNull();
    }

    [Fact]
    public void create_with_string_content()
    {
        var message = Message.Create("hello world", queue: "test");

        var deserialized = message.GetContent<string>();
        deserialized.ShouldBe("hello world");
    }

    [Fact]
    public void create_with_anonymous_type_and_deserialize_to_dictionary()
    {
        var content = new { Name = "Test", Value = 42 };

        var message = Message.Create(content, queue: "test");

        var deserialized = message.GetContent<Dictionary<string, object>>();
        deserialized.ShouldNotBeNull();
    }

    [Fact]
    public void create_with_delivery_options()
    {
        var order = new OrderMessage("ORD-003", 30.00m, "USD");
        var deliverBy = DateTime.UtcNow.AddMinutes(5);

        var message = Message.Create(
            order,
            queue: "priority",
            deliverBy: deliverBy,
            maxAttempts: 3,
            destinationUri: "lq.tcp://localhost:5050"
        );

        message.DeliverBy.ShouldBe(deliverBy);
        message.MaxAttempts.ShouldBe(3);
        message.GetContent<OrderMessage>()!.OrderId.ShouldBe("ORD-003");
    }
}
