using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using Foundatio.AsyncEx;
using Foundatio.Messaging;
using Foundatio.Tests.Extensions;
using Foundatio.Tests.Messaging;
using RabbitMQ.Client;
using Xunit;

namespace Foundatio.RabbitMQ.Tests.Messaging;

public abstract class RabbitMqPriorityBehaviorTestBase(string connectionString, ITestOutputHelper output) : MessageBusTestBase(output)
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PublishAsync_BeforeSubscription_DoesNotRetainUnroutableMessages(bool quorum)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "RabbitMQ infrastructure not available");

        // Arrange
        await using var bus = CreateBus(quorum);
        var received = new ConcurrentQueue<string>();
        var delivered = new AsyncManualResetEvent();
        try
        {
            await bus.PublishAsync(new SimpleMessageA { Data = "before" }, cancellationToken: TestCancellationToken);

            // Act
            await bus.SubscribeAsync<SimpleMessageA>(message =>
            {
                received.Enqueue(message.Data!);
                delivered.Set();
            }, TestCancellationToken);
            await bus.PublishAsync(new SimpleMessageA { Data = "after" }, cancellationToken: TestCancellationToken);
            await delivered.WaitAsync(TestCancellationToken).WaitAsync(TimeSpan.FromSeconds(10), TestCancellationToken);

            // Assert
            Assert.Equal(["after"], received.ToArray());
        }
        finally
        {
            await CleanupMessageBusAsync(bus);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PublishAsync_WithAnInFlightLowPriorityMessage_DoesNotPreemptDelivery(bool quorum)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "RabbitMQ infrastructure not available");

        // Arrange
        await using var bus = CreateBus(quorum);
        var received = new ConcurrentQueue<string>();
        var lowReceived = new AsyncManualResetEvent();
        var releaseLow = new AsyncManualResetEvent();
        var countdown = new AsyncCountdownEvent(2);
        try
        {
            await bus.SubscribeAsync<SimpleMessageA>(async message =>
            {
                received.Enqueue(message.Data!);
                if (message.Data == "low")
                {
                    lowReceived.Set();
                    await releaseLow.WaitAsync(TestCancellationToken);
                }
                countdown.Signal();
            }, TestCancellationToken);
            await bus.PublishAsync(new SimpleMessageA { Data = "low" },
                new MessageOptions { Properties = { ["Priority"] = "1" } }, TestCancellationToken);
            await lowReceived.WaitAsync(TestCancellationToken).WaitAsync(TimeSpan.FromSeconds(10), TestCancellationToken);

            // Act
            await bus.PublishAsync(new SimpleMessageA { Data = "high" },
                new MessageOptions { Properties = { ["Priority"] = "10" } }, TestCancellationToken);
            releaseLow.Set();
            await countdown.WaitAsync(TimeSpan.FromSeconds(10));

            // Assert
            Assert.Equal(["low", "high"], received.ToArray());
        }
        finally
        {
            releaseLow.Set();
            await CleanupMessageBusAsync(bus);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PublishAsync_WithOmittedOrZeroPriority_UsesQueueSpecificDefault(bool quorum)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "RabbitMQ infrastructure not available");

        // Arrange
        var version = await GetBrokerVersionAsync();

        // Act
        var received = await PublishQueuedAsync(quorum, ("one", "1"), ("omitted", null), ("zero", "0"));

        // Assert
        // RabbitMQ.Client 7.2.2 omits priority zero; quorum 4.3 defaults an absent priority to 4.
        Assert.Equal(quorum && version >= new Version(4, 3)
            ? ["omitted", "zero", "one"]
            : ["one", "omitted", "zero"], received);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PublishAsync_WithQueuedPriorities_UsesQueueSpecificPriorityLevels(bool quorum)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "RabbitMQ infrastructure not available");

        // Arrange
        var version = await GetBrokerVersionAsync();

        // Act
        var received = await PublishQueuedAsync(quorum,
            ("five-a", "5"), ("ten-a", "10"), ("five-b", "5"), ("ten-b", "10"));

        // Assert
        // Before 4.3 these quorum messages share one priority group (or FIFO before 4.0).
        Assert.Equal(!quorum || version >= new Version(4, 3)
            ? ["ten-a", "ten-b", "five-a", "five-b"]
            : ["five-a", "ten-a", "five-b", "ten-b"], received);
    }

    [Fact]
    public async Task PublishAsync_WithQueuedQuorumPriorities_UsesVersionSpecificFairness()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "RabbitMQ infrastructure not available");

        // Arrange
        var version = await GetBrokerVersionAsync();

        // Act
        var received = await PublishQueuedAsync(true, ("normal", "1"),
            ("high-a", "10"), ("high-b", "10"), ("high-c", "10"),
            ("high-d", "10"), ("high-e", "10"), ("high-f", "10"));

        // Assert
        Assert.Equal(7, received.Length);
        Assert.Equal(["high-a", "high-b", "high-c", "high-d", "high-e", "high-f"],
            Array.FindAll(received, message => message != "normal"));
        if (version >= new Version(4, 3))
            Assert.Equal("normal", received[^1]);
        else if (version >= new Version(4, 0))
        {
            Assert.StartsWith("high-", received[0]);
            Assert.InRange(Array.IndexOf(received, "normal"), 1, 5);
        }
        else
            Assert.Equal("normal", received[0]);
    }

    private RabbitMQMessageBus CreateBus(bool quorum)
    {
        return new RabbitMQMessageBus(options =>
        {
            options.ConnectionString(connectionString)
                .Topic($"priority-behavior-{Guid.NewGuid():N}")
                .SubscriptionQueueName($"priority-behavior-{Guid.NewGuid():N}")
                .AcknowledgementStrategy(AcknowledgementStrategy.Automatic)
                .PrefetchCount(1)
                .PublisherConfirmsEnabled()
                .LoggerFactory(Log);
            return quorum ? options.UseQuorumQueues() : options.UseMessagePriority(10);
        });
    }

    private async Task<Version> GetBrokerVersionAsync()
    {
        var factory = new ConnectionFactory { Uri = new Uri(connectionString) };
        await using var connection = await factory.CreateConnectionAsync(TestCancellationToken);
        var version = RabbitMQMessageBus.ParseServerVersion(connection.ServerProperties);
        Assert.NotNull(version);
        return version;
    }

    private async Task<string[]> PublishQueuedAsync(bool quorum, params (string Data, string? Priority)[] messages)
    {
        await using var bus = CreateBus(quorum);
        var received = new ConcurrentQueue<string>();
        var countdown = new AsyncCountdownEvent(messages.Length);
        var warmupReceived = new AsyncManualResetEvent();
        var releaseWarmup = new AsyncManualResetEvent();
        try
        {
            await bus.SubscribeAsync<SimpleMessageA>(async message =>
            {
                if (message.Data == "warmup")
                {
                    warmupReceived.Set();
                    await releaseWarmup.WaitAsync(TestCancellationToken);
                    return;
                }

                received.Enqueue(message.Data!);
                countdown.Signal();
            }, TestCancellationToken);

            // Fill the single delivery slot so priorities are compared while messages are queued.
            await bus.PublishAsync(new SimpleMessageA { Data = "warmup" }, cancellationToken: TestCancellationToken);
            await warmupReceived.WaitAsync(TestCancellationToken).WaitAsync(TimeSpan.FromSeconds(10), TestCancellationToken);
            foreach (var message in messages)
            {
                var options = new MessageOptions();
                if (message.Priority is not null)
                    options.Properties["Priority"] = message.Priority;
                await bus.PublishAsync(new SimpleMessageA { Data = message.Data }, options, TestCancellationToken);
            }

            releaseWarmup.Set();
            await countdown.WaitAsync(TimeSpan.FromSeconds(10));
            return received.ToArray();
        }
        finally
        {
            releaseWarmup.Set();
            await CleanupMessageBusAsync(bus);
        }
    }
}
