using System;
using System.Threading;
using System.Threading.Tasks;
using Foundatio.AsyncEx;
using Foundatio.Messaging;
using Foundatio.Tests.Extensions;
using Foundatio.Tests.Messaging;
using Microsoft.Extensions.Logging;
using Xunit;
using Xunit.Abstractions;

namespace Foundatio.RabbitMQ.Tests.Messaging;

public abstract class RabbitMqMessageBusTestBase(string connectionString, ITestOutputHelper output) : MessageBusTestBase(output)
{
    private readonly string _topic = $"test_topic_{DateTime.UtcNow.Ticks}";

    protected override IMessageBus GetMessageBus(Func<SharedMessageBusOptions, SharedMessageBusOptions> config = null)
    {
        return new RabbitMQMessageBus(o =>
        {
            o.ConnectionString(connectionString);
            o.LoggerFactory(Log);

            config?.Invoke(o.Target);

            return o;
        });
    }

    [Fact]
    public override Task CanUseMessageOptionsAsync()
    {
        return base.CanUseMessageOptionsAsync();
    }

    [Fact]
    public override Task CanSendMessageAsync()
    {
        return base.CanSendMessageAsync();
    }

    [Fact]
    public override Task CanHandleNullMessageAsync()
    {
        return base.CanHandleNullMessageAsync();
    }

    [Fact]
    public override Task CanSendDerivedMessageAsync()
    {
        return base.CanSendDerivedMessageAsync();
    }

    [Fact]
    public override Task CanSendMappedMessageAsync()
    {
        return base.CanSendMappedMessageAsync();
    }

    [Fact]
    public override Task CanSendDelayedMessageAsync()
    {
        return base.CanSendDelayedMessageAsync();
    }

    [Fact]
    public override Task CanSubscribeConcurrentlyAsync()
    {
        return base.CanSubscribeConcurrentlyAsync();
    }

    [Fact]
    public override Task CanReceiveMessagesConcurrentlyAsync()
    {
        return base.CanReceiveMessagesConcurrentlyAsync();
    }

    [Fact]
    public override Task CanSendMessageToMultipleSubscribersAsync()
    {
        return base.CanSendMessageToMultipleSubscribersAsync();
    }

    [Fact]
    public override Task CanTolerateSubscriberFailureAsync()
    {
        return base.CanTolerateSubscriberFailureAsync();
    }

    [Fact]
    public override Task WillOnlyReceiveSubscribedMessageTypeAsync()
    {
        return base.WillOnlyReceiveSubscribedMessageTypeAsync();
    }

    [Fact]
    public override Task WillReceiveDerivedMessageTypesAsync()
    {
        return base.WillReceiveDerivedMessageTypesAsync();
    }

    [Fact]
    public override Task CanSubscribeToAllMessageTypesAsync()
    {
        return base.CanSubscribeToAllMessageTypesAsync();
    }

    [Fact]
    public override Task CanSubscribeToRawMessagesAsync()
    {
        return base.CanSubscribeToRawMessagesAsync();
    }

    [Fact]
    public override Task CanCancelSubscriptionAsync()
    {
        return base.CanCancelSubscriptionAsync();
    }

    [Fact]
    public override Task WontKeepMessagesWithNoSubscribersAsync()
    {
        return base.WontKeepMessagesWithNoSubscribersAsync();
    }

    [Fact]
    public override Task CanReceiveFromMultipleSubscribersAsync()
    {
        return base.CanReceiveFromMultipleSubscribersAsync();
    }

    [Fact]
    public override void CanDisposeWithNoSubscribersOrPublishers()
    {
        base.CanDisposeWithNoSubscribersOrPublishers();
    }

    [Fact]
    public async Task CanPersistAndNotLoseMessages()
    {
        var messageBus1 = new RabbitMQMessageBus(o => o
            .ConnectionString(connectionString)
            .LoggerFactory(Log)
            .SubscriptionQueueName($"{_topic}-offline")
            .IsSubscriptionQueueExclusive(false)
            .SubscriptionQueueAutoDelete(false)
            .AcknowledgementStrategy(AcknowledgementStrategy.Automatic));

        var countdownEvent = new AsyncCountdownEvent(1);
        var cts = new CancellationTokenSource();
        await messageBus1.SubscribeAsync<SimpleMessageA>(msg =>
        {
            _logger.LogInformation("[Subscriber1] Got message: {Message}", msg.Data);
            countdownEvent.Signal();
        }, cts.Token);

        await messageBus1.PublishAsync(new SimpleMessageA { Data = "Audit message 1" });
        await countdownEvent.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(0, countdownEvent.CurrentCount);
        await cts.CancelAsync();

        await messageBus1.PublishAsync(new SimpleMessageA { Data = "Audit message 2" });

        cts = new CancellationTokenSource();
        countdownEvent.AddCount(1);
        await messageBus1.SubscribeAsync<SimpleMessageA>(msg =>
        {
            _logger.LogInformation("[Subscriber2] Got message: {Message}", msg.Data);
            countdownEvent.Signal();
        }, cts.Token);
        await countdownEvent.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(0, countdownEvent.CurrentCount);
        await cts.CancelAsync();

        await messageBus1.PublishAsync(new SimpleMessageA { Data = "Audit offline message 1" });
        await messageBus1.PublishAsync(new SimpleMessageA { Data = "Audit offline message 2" });
        await messageBus1.PublishAsync(new SimpleMessageA { Data = "Audit offline message 3" });

        await messageBus1.DisposeAsync();

        var messageBus2 = new RabbitMQMessageBus(o => o
            .ConnectionString(connectionString)
            .LoggerFactory(Log)
            .SubscriptionQueueName($"{_topic}-offline")
            .IsSubscriptionQueueExclusive(false)
            .SubscriptionQueueAutoDelete(false)
            .AcknowledgementStrategy(AcknowledgementStrategy.Automatic));

        cts = new CancellationTokenSource();
        countdownEvent.AddCount(4);
        await messageBus2.SubscribeAsync<SimpleMessageA>(msg =>
        {
            _logger.LogInformation("[Subscriber3] Got message: {Message}", msg.Data);
            countdownEvent.Signal();
        }, cts.Token);
        await messageBus2.PublishAsync(new SimpleMessageA { Data = "Another audit message 4" });
        await countdownEvent.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(0, countdownEvent.CurrentCount);

        await messageBus2.DisposeAsync();
    }
}