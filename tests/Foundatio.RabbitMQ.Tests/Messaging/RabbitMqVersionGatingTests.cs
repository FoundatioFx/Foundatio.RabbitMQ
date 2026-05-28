using System;
using System.Threading.Tasks;
using Foundatio.Messaging;
using Foundatio.Tests.Messaging;
using Foundatio.Xunit;
using Xunit;

namespace Foundatio.RabbitMQ.Tests.Messaging;

public class RabbitMqVersionGatingTests(AspireFixture fixture, ITestOutputHelper output)
    : TestWithLoggingBase(output), IClassFixture<AspireFixture>
{
    [Fact]
    public async Task SubscribeAsync_WithPerChannelQos_DeliversMessages()
    {
        string topic = "versiongate-globalqos-" + Guid.NewGuid().ToString("N")[..8];

        await using var messageBus = new RabbitMQMessageBus(o => o
            .ConnectionString(fixture.MessagingConnectionString!)
            .Topic(topic)
            .PrefetchCount(10)
            .UseQuorumQueues()
            .LoggerFactory(Log));

        string? receivedData = null;
        await messageBus.SubscribeAsync<SimpleMessageA>(msg =>
        {
            receivedData = msg.Data;
        }, TestCancellationToken);

        await Task.Delay(TimeSpan.FromSeconds(1), TestCancellationToken);

        await messageBus.PublishAsync(new SimpleMessageA { Data = "globalqos-fallback" },
            cancellationToken: TestCancellationToken);

        await Task.Delay(TimeSpan.FromSeconds(3), TestCancellationToken);
        Assert.Equal("globalqos-fallback", receivedData);
    }

    [Fact]
    public async Task PublishAsync_WithConfirmsAndVersionDetection_DeliversSuccessfully()
    {
        string topic = "versiongate-confirms-" + Guid.NewGuid().ToString("N")[..8];

        await using var messageBus = new RabbitMQMessageBus(o => o
            .ConnectionString(fixture.MessagingConnectionString!)
            .Topic(topic)
            .PublisherConfirmsEnabled(true)
            .UseQuorumQueues()
            .LoggerFactory(Log));

        string? receivedData = null;
        await messageBus.SubscribeAsync<SimpleMessageA>(msg =>
        {
            receivedData = msg.Data;
        }, TestCancellationToken);

        await Task.Delay(TimeSpan.FromSeconds(1), TestCancellationToken);

        await messageBus.PublishAsync(new SimpleMessageA { Data = "confirmed" },
            cancellationToken: TestCancellationToken);

        await Task.Delay(TimeSpan.FromSeconds(3), TestCancellationToken);
        Assert.Equal("confirmed", receivedData);
    }

    [Fact]
    public async Task SubscribeAsync_WithQuorumQueueAndDeliveryLimit_DeliversMessages()
    {
        string topic = "versiongate-delivery-" + Guid.NewGuid().ToString("N")[..8];

        await using var messageBus = new RabbitMQMessageBus(o => o
            .ConnectionString(fixture.MessagingConnectionString!)
            .Topic(topic)
            .UseQuorumQueues()
            .DeliveryLimit(3)
            .LoggerFactory(Log));

        string? receivedData = null;
        await messageBus.SubscribeAsync<SimpleMessageA>(msg =>
        {
            receivedData = msg.Data;
        }, TestCancellationToken);

        await Task.Delay(TimeSpan.FromSeconds(1), TestCancellationToken);

        await messageBus.PublishAsync(new SimpleMessageA { Data = "delivery-limit" },
            cancellationToken: TestCancellationToken);

        await Task.Delay(TimeSpan.FromSeconds(3), TestCancellationToken);
        Assert.Equal("delivery-limit", receivedData);
    }
}
