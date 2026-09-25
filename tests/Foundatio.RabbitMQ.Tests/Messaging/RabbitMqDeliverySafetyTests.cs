using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Foundatio.Messaging;
using Foundatio.Tests.Messaging;
using Foundatio.Utility;
using Foundatio.Xunit;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Xunit;

namespace Foundatio.RabbitMQ.Tests.Messaging;

[Collection(nameof(RabbitMqTestCollection))]
public class RabbitMqDeliverySafetyTests(AspireFixture fixture, ITestOutputHelper output) : TestWithLoggingBase(output)
{
    [Fact]
    public async Task PublishAsync_AfterChannelClosure_RecreatesPublisherChannelAsync()
    {
        Assert.SkipWhen(!fixture.IsAvailable, "RabbitMQ infrastructure not available");

        // Arrange
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(TestCancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(30));
        string topic = $"safety-publisher-{Guid.NewGuid():N}";
        var received = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var bus = CreateBus(topic, "subscriber");
        await bus.SubscribeAsync<SimpleMessageA>(message => received.TrySetResult(message.Data!), timeout.Token);
        var connection = GetField<IConnection>(bus, "_publisherConnection");
        // Act
        await GetField<IChannel>(bus, "_publisherChannel").CloseAsync(200, "Synthetic publisher channel fault", cancellationToken: timeout.Token);
        Assert.True(connection.IsOpen);
        await bus.PublishAsync(new SimpleMessageA { Data = topic }, cancellationToken: timeout.Token);
        // Assert
        Assert.Equal(topic, await received.Task.WaitAsync(timeout.Token));
    }

    [Fact]
    public async Task PublishAsync_WithDeletedDelayedExchange_RecreatesItAfterChannelClosureAsync()
    {
        Assert.SkipWhen(!fixture.IsAvailable, "RabbitMQ infrastructure not available");

        // Arrange
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(TestCancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(40));
        string topic = $"safety-delayed-{Guid.NewGuid():N}";
        var factory = new ConnectionFactory { Uri = new Uri(fixture.MessagingDelayedConnectionString!) };
        await using var connection = await factory.CreateConnectionAsync(timeout.Token);
        await using var admin = await connection.CreateChannelAsync(cancellationToken: timeout.Token);
        await using var bus = new RabbitMQMessageBus(new RabbitMQMessageBusOptions
        {
            ConnectionString = fixture.MessagingDelayedConnectionString,
            Topic = topic,
            IsDurable = true,
            PublisherConfirmsEnabled = true,
            RequireBrokerDelayedDelivery = true,
            LoggerFactory = Log
        });
        var options = new MessageOptions { DeliveryDelay = TimeSpan.FromMinutes(1) };
        try
        {
            await bus.PublishAsync(new SimpleMessageA { Data = "first" }, options, timeout.Token);
            await admin.ExchangeDeleteAsync(topic, cancellationToken: timeout.Token);

            // Act
            await GetField<IChannel>(bus, "_publisherChannel").CloseAsync(200, "Synthetic channel fault", cancellationToken: timeout.Token);
            await bus.PublishAsync(new SimpleMessageA { Data = "second" }, options, timeout.Token);

            // Assert
            await admin.ExchangeDeclarePassiveAsync(topic, cancellationToken: timeout.Token);
            Assert.True(admin.IsOpen);
        }
        finally
        {
            await admin.ExchangeDeleteAsync(topic, cancellationToken: timeout.Token);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SubscribeAsync_AfterChannelClosureOrConsumerCancellation_ResumesWithoutRestartAsync(bool cancelConsumer)
    {
        Assert.SkipWhen(!fixture.IsAvailable, "RabbitMQ infrastructure not available");

        // Arrange
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(TestCancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(40));
        string topic = $"safety-lifecycle-{Guid.NewGuid():N}";
        var received = new ConcurrentDictionary<string, byte>();
        await using var bus = CreateBus(topic, "consumer");
        await bus.SubscribeAsync<SimpleMessageA>(message => received.TryAdd(message.Data!, 0), timeout.Token);
        await bus.PublishAsync(new SimpleMessageA { Data = "before" }, cancellationToken: timeout.Token);
        await WaitAsync(() => received.ContainsKey("before"), timeout.Token);
        var connection = GetField<IConnection>(bus, "_subscriberConnection");
        var channel = GetField<IChannel>(bus, "_subscriberChannel");
        var consumer = GetField<AsyncEventingBasicConsumer>(bus, "_consumer");
        // Act
        if (cancelConsumer)
            await channel.BasicCancelAsync(Assert.Single(consumer.ConsumerTags), cancellationToken: timeout.Token);
        else
            await channel.CloseAsync(200, "Synthetic channel-only fault", cancellationToken: timeout.Token);
        Assert.True(connection.IsOpen, "The test must not substitute TCP/network recovery for a channel/consumer fault.");

        await WaitAsync(() =>
        {
            var current = typeof(RabbitMQMessageBus).GetField("_consumer", BindingFlags.Instance | BindingFlags.NonPublic)?.GetValue(bus) as AsyncEventingBasicConsumer;
            return current is not null && !ReferenceEquals(consumer, current) && current.IsRunning;
        }, timeout.Token);
        await bus.PublishAsync(new SimpleMessageA { Data = "after" }, cancellationToken: timeout.Token);
        await WaitAsync(() => received.ContainsKey("after"), timeout.Token);
        // Assert
        Assert.Equal(new[] { "after", "before" }, received.Keys.OrderBy(value => value));
    }

    [Fact]
    public async Task SubscribeAsync_WhenQueueTypeChangesAfterConstruction_RejectsPriorityAsync()
    {
        Assert.SkipWhen(!fixture.IsAvailable, "RabbitMQ infrastructure not available");

        // Arrange
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(TestCancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(30));
        string topic = $"safety-priority-options-{Guid.NewGuid():N}";
        var arguments = new Dictionary<string, object?> { [RabbitMQConstants.QueueTypeArgument] = "classic" };
        await using var bus = new RabbitMQMessageBus(new RabbitMQMessageBusOptions
        {
            ConnectionString = fixture.MessagingConnectionString,
            LoggerFactory = Log,
            Topic = topic,
            SubscriptionQueueName = $"{topic}-consumer",
            MaxPriority = 3,
            Arguments = arguments
        });
        var factory = new ConnectionFactory { Uri = new Uri(fixture.MessagingConnectionString!) };
        await using var adminConnection = await factory.CreateConnectionAsync(timeout.Token);
        await using var admin = await adminConnection.CreateChannelAsync(cancellationToken: timeout.Token);
        try
        {
            arguments[RabbitMQConstants.QueueTypeArgument] = "quorum";

            // Act
            var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
                bus.SubscribeAsync<SimpleMessageA>(_ => { }, timeout.Token));

            // Assert
            Assert.Contains("queue type", exception.Message, StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            await admin.ExchangeDeleteAsync(topic, cancellationToken: timeout.Token);
        }
    }

    [Fact]
    public async Task SubscribeAsync_WithFailingHandler_RetriesOnlyFailedSubscriptionWithStableIdentityAsync()
    {
        Assert.SkipWhen(!fixture.IsAvailable, "RabbitMQ infrastructure not available");

        // Arrange
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(TestCancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(30));
        string topic = $"safety-retry-{Guid.NewGuid():N}";
        string id = Guid.NewGuid().ToString("N");
        var successes = new ConcurrentQueue<string>();
        var retries = new ConcurrentQueue<string>();
        var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var successful = CreateBus(topic, "successful");
        await using var failing = CreateBus(topic, "failing");
        await successful.SubscribeAsync<IMessage<SimpleMessageA>>(message => successes.Enqueue(message.UniqueId!), timeout.Token);
        await failing.SubscribeAsync<IMessage<SimpleMessageA>>(message =>
        {
            retries.Enqueue(message.UniqueId!);
            if (retries.Count == 1)
                throw new InvalidOperationException("Synthetic first-attempt failure");
            done.TrySetResult();
        }, timeout.Token);

        // Act
        await successful.PublishAsync(new SimpleMessageA { Data = id }, new MessageOptions { UniqueId = id }, timeout.Token);
        await done.Task.WaitAsync(timeout.Token);
        // Complete the transport shutdown before inspecting the final callback inventory.
        await failing.DisposeAsync();
        await successful.DisposeAsync();
        // Assert
        Assert.Equal(new[] { id }, successes.ToArray());
        Assert.Equal(new[] { id, id }, retries.ToArray());
    }

    private RabbitMQMessageBus CreateBus(string topic, string suffix) => new(new RabbitMQMessageBusOptions
    {
        ConnectionString = fixture.MessagingConnectionString,
        LoggerFactory = Log,
        Topic = topic,
        SubscriptionQueueName = $"{topic}-{suffix}",
        IsDurable = false,
        IsSubscriptionQueueExclusive = false,
        SubscriptionQueueAutoDelete = true,
        AcknowledgementStrategy = AcknowledgementStrategy.Automatic,
        PrefetchCount = 1,
        PublisherConfirmsEnabled = true,
        NetworkRecoveryInterval = TimeSpan.FromSeconds(1),
        Arguments = new Dictionary<string, object?> { ["x-queue-type"] = "classic" }
    });

    private static T GetField<T>(RabbitMQMessageBus bus, string name)
    {
        var field = typeof(RabbitMQMessageBus).GetField(name, BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(field);
        return Assert.IsAssignableFrom<T>(field.GetValue(bus));
    }

    private static async Task WaitAsync(Func<bool> condition, CancellationToken cancellationToken)
    {
        while (!condition())
            await Task.Delay(TimeSpan.FromMilliseconds(50), cancellationToken);
    }
}
