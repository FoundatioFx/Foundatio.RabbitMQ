using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Foundatio.AsyncEx;
using Foundatio.Messaging;
using Foundatio.Tests.Extensions;
using Foundatio.Tests.Messaging;
using Microsoft.Extensions.Logging;
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
        await using var bus = new RabbitMQMessageBus(o => o
            .ConnectionString(ConnectionString)
            .Topic(topic)
            .SubscriptionQueueName(queueName)
            .AcknowledgementStrategy(AcknowledgementStrategy.Automatic)
            .UseMessagePriority(10)
            .PrefetchCount(1)
            .PublisherConfirmsEnabled()
            .LoggerFactory(Log));
        var received = new ConcurrentQueue<string>();
        var countdownEvent = new AsyncCountdownEvent(3);
        var warmupReceived = new AsyncManualResetEvent();
        var releaseWarmup = new AsyncManualResetEvent();

        try
        {
            await bus.SubscribeAsync<SimpleMessageA>(async msg =>
            {
                if (msg.Data == "warmup")
                {
                    warmupReceived.Set();
                    await releaseWarmup.WaitAsync(TestCancellationToken);
                    return;
                }

                received.Enqueue(msg.Data!);
                countdownEvent.Signal();
            }, TestCancellationToken);

            // Hold the only prefetched delivery so the priority messages wait in the queue.
            await bus.PublishAsync(new SimpleMessageA { Data = "warmup" }, cancellationToken: TestCancellationToken);
            await warmupReceived.WaitAsync(TestCancellationToken).WaitAsync(TimeSpan.FromSeconds(10), TestCancellationToken);
            await bus.PublishAsync(new SimpleMessageA { Data = "low" },
                new MessageOptions { Properties = { ["Priority"] = "1" } }, TestCancellationToken);
            await bus.PublishAsync(new SimpleMessageA { Data = "high" },
                new MessageOptions { Properties = { ["Priority"] = "10" } }, TestCancellationToken);
            await bus.PublishAsync(new SimpleMessageA { Data = "medium" },
                new MessageOptions { Properties = { ["Priority"] = "5" } }, TestCancellationToken);

            // Act
            releaseWarmup.Set();
            await countdownEvent.WaitAsync(TimeSpan.FromSeconds(10));

            // Assert
            Assert.Equal(["high", "medium", "low"], received.ToArray());
        }
        finally
        {
            releaseWarmup.Set();
            await CleanupMessageBusAsync(bus);
        }
    }
}
