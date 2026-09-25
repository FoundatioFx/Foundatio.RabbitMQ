using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Foundatio.AsyncEx;
using Foundatio.Messaging;
using Foundatio.Tests.Extensions;
using Foundatio.Tests.Messaging;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using Xunit;

namespace Foundatio.RabbitMQ.Tests.Messaging;

public abstract class RabbitMqMessageBusClassicTestBase : RabbitMqMessageBusTestBase
{
    public RabbitMqMessageBusClassicTestBase(string connectionString, ITestOutputHelper output) : base(connectionString, output)
    {
    }


    protected override IMessageBus? GetMessageBus(Func<SharedMessageBusOptions, SharedMessageBusOptions>? config = null)
    {
        if (string.IsNullOrEmpty(ConnectionString))
            return null;

        return new RabbitMQMessageBus(o =>
        {
            o.ConnectionString(ConnectionString);
            o.LoggerFactory(Log);

            config?.Invoke(o.Target);

            return o;
        });
    }

    [Fact]
    public override async Task CanHandlePoisonedMessageWithAutomaticAcknowledgementsAsync()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "RabbitMQ infrastructure not available");

        await using var messageBus = new RabbitMQMessageBus(o => o
            .ConnectionString(ConnectionString)
            .LoggerFactory(Log)
            .AcknowledgementStrategy(AcknowledgementStrategy.Automatic));

        long handlerInvocations = 0;

        try
        {
            await messageBus.SubscribeAsync<SimpleMessageA>(_ =>
            {
                _logger.LogTrace("SimpleAMessage received");
                Interlocked.Increment(ref handlerInvocations);
                throw new Exception("Poisoned message");
            }, TestCancellationToken);

            await messageBus.PublishAsync(new SimpleMessageA(), cancellationToken: TestCancellationToken);
            _logger.LogTrace("Published one...");

            await Task.Delay(TimeSpan.FromSeconds(3), TestCancellationToken);
            Assert.Equal(3, handlerInvocations);
        }
        finally
        {
            await CleanupMessageBusAsync(messageBus);
        }
    }

    [Fact]
    public async Task PublishAsync_WithClassicPriority_DeliversHighPriorityFirst()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "RabbitMQ infrastructure not available");

        // Arrange
        string topic = $"test_topic_classic_priority_{Guid.NewGuid():N}";
        string queueName = $"{topic}_queue";
        var factory = new ConnectionFactory { Uri = new Uri(ConnectionString) };
        await using var connection = await factory.CreateConnectionAsync(TestCancellationToken);
        await using var channel = await connection.CreateChannelAsync(cancellationToken: TestCancellationToken);
        try
        {
            await channel.ExchangeDeclareAsync(topic, "fanout", durable: true, cancellationToken: TestCancellationToken);
            await channel.QueueDeclareAsync(queueName, durable: true, exclusive: false, autoDelete: false,
                arguments: new Dictionary<string, object?> { ["x-queue-type"] = "classic", ["x-max-priority"] = 10 },
                cancellationToken: TestCancellationToken);
            await channel.QueueBindAsync(queueName, topic, String.Empty, cancellationToken: TestCancellationToken);

            await using var bus = new RabbitMQMessageBus(o => o
                .ConnectionString(ConnectionString)
                .Topic(topic)
                .SubscriptionQueueName(queueName)
                .AcknowledgementStrategy(AcknowledgementStrategy.Automatic)
                .IsSubscriptionQueueExclusive(false)
                .SubscriptionQueueAutoDelete(false)
                .Arguments(new Dictionary<string, object?> { ["x-queue-type"] = "classic" })
                .UseMessagePriority(10)
                .PrefetchCount(1)
                .PublisherConfirmsEnabled()
                .LoggerFactory(Log));
            await bus.PublishAsync(new SimpleMessageA { Data = "low" },
                new MessageOptions { Properties = { ["Priority"] = "1" } }, TestCancellationToken);
            await bus.PublishAsync(new SimpleMessageA { Data = "high" },
                new MessageOptions { Properties = { ["Priority"] = "10" } }, TestCancellationToken);
            await bus.PublishAsync(new SimpleMessageA { Data = "medium" },
                new MessageOptions { Properties = { ["Priority"] = "5" } }, TestCancellationToken);
            var received = new ConcurrentQueue<string>();
            var countdownEvent = new AsyncCountdownEvent(3);

            // Act
            await bus.SubscribeAsync<SimpleMessageA>(msg =>
            {
                received.Enqueue(msg.Data!);
                countdownEvent.Signal();
            }, TestCancellationToken);
            await countdownEvent.WaitAsync(TimeSpan.FromSeconds(10));

            // Assert
            Assert.Equal(["high", "medium", "low"], received.ToArray());
        }
        finally
        {
            using var cleanup = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            await channel.QueueDeleteAsync(queueName, cancellationToken: cleanup.Token);
            await channel.ExchangeDeleteAsync(topic, cancellationToken: cleanup.Token);
        }
    }
}
