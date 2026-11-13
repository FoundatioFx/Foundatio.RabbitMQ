using System;
using System.Collections.Concurrent;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Foundatio.Messaging;
using Foundatio.Tests.Messaging;
using Foundatio.Xunit;
using RabbitMQ.Client;
using Xunit;

namespace Foundatio.RabbitMQ.Tests.Messaging;

[Collection(nameof(RabbitMqTestCollection))]
public class RabbitMqRequiredDeliveryTests(AspireFixture fixture, ITestOutputHelper output) : TestWithLoggingBase(output)
{
    [Fact]
    public async Task PublishAsync_WithBrokerDelay_SurvivesPublisherDisposalAsync()
    {
        Assert.SkipWhen(String.IsNullOrEmpty(fixture.MessagingDelayedConnectionString), "Delayed broker unavailable");

        // Arrange
        using var timeout = CreateTimeout();
        await using var context = await RabbitMqReliabilityTestContext.CreateAsync(fixture.MessagingDelayedConnectionString!, Log, timeout.Token);
        var options = context.Options();
        options.RequireBrokerDelayedDelivery = true;
        await using (var provision = new RabbitMQMessageBus(options))
            await provision.SubscribeAsync<SimpleMessageA>(_ => { }, timeout.Token);
        string id = Guid.NewGuid().ToString("N");
        // Act
        await using (var publisher = new RabbitMQMessageBus(options))
            await publisher.PublishAsync(new SimpleMessageA { Data = id },
                new MessageOptions { UniqueId = id, DeliveryDelay = TimeSpan.FromSeconds(2) }, timeout.Token);
        var received = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var subscriber = new RabbitMQMessageBus(options);
        await subscriber.SubscribeAsync<SimpleMessageA>(message => received.TrySetResult(message.Data!), timeout.Token);
        // Assert
        Assert.Equal(id, await received.Task.WaitAsync(timeout.Token));
        await RabbitMqReliabilityTestContext.WaitAsync(() => subscriber.ActiveDeliveryCount == 0, timeout.Token);
    }

    [Fact]
    public async Task PublishAsync_WithRequiredBrokerDelayAndNoPlugin_RejectsMemoryFallbackAsync()
    {
        Assert.SkipWhen(!fixture.IsAvailable, "RabbitMQ infrastructure not available");

        // Arrange
        using var timeout = CreateTimeout();
        await using var context = await RabbitMqReliabilityTestContext.CreateAsync(fixture.MessagingConnectionString!, Log, timeout.Token);
        var options = context.Options();
        options.RequireBrokerDelayedDelivery = true;
        var receipts = new ConcurrentQueue<string>();
        await using var bus = new RabbitMQMessageBus(options);
        await bus.SubscribeAsync<SimpleMessageA>(message => receipts.Enqueue(message.Data!), timeout.Token);
        // Act
        // Assert
        await Assert.ThrowsAnyAsync<MessageBusException>(() => bus.PublishAsync(new SimpleMessageA { Data = "must-not-schedule" },
            new MessageOptions { DeliveryDelay = TimeSpan.FromMilliseconds(100) }, timeout.Token));
        await Task.Delay(TimeSpan.FromMilliseconds(300), timeout.Token);
        Assert.Empty(receipts);
    }

    [Fact]
    public async Task PublishAsync_WithRequiredRoutingAndNoSubscription_ThrowsMessageBusExceptionAsync()
    {
        Assert.SkipWhen(!fixture.IsAvailable, "RabbitMQ infrastructure not available");

        // Arrange
        using var timeout = CreateTimeout();
        await using var context = await RabbitMqReliabilityTestContext.CreateAsync(fixture.MessagingConnectionString!, Log, timeout.Token);
        var options = context.Options();
        options.RequirePublishRouting = true;
        options.PublisherConfirmsEnabled = false;
        await using var bus = new RabbitMQMessageBus(options);
        // Act
        // Assert
        await Assert.ThrowsAnyAsync<MessageBusException>(() => bus.PublishAsync(
            new SimpleMessageA { Data = "unroutable" }, cancellationToken: timeout.Token));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SubscribeAsync_WithExhaustedDeliveryAndNoDestination_RetainsAcrossDisposalAsync(bool quorum)
    {
        Assert.SkipWhen(!fixture.IsAvailable, "RabbitMQ infrastructure not available");

        // Arrange
        using var timeout = CreateTimeout();
        await using var context = await RabbitMqReliabilityTestContext.CreateAsync(fixture.MessagingConnectionString!, Log, timeout.Token);
        var options = context.Options(quorum);
        options.DeliveryLimit = 2;
        int attempts = 0;
        await using var bus = new RabbitMQMessageBus(options);
        await bus.SubscribeAsync<SimpleMessageA>(_ =>
        {
            Interlocked.Increment(ref attempts);
            throw new InvalidOperationException("Synthetic poisoned event");
        }, timeout.Token);
        string id = Guid.NewGuid().ToString("N");
        // Act
        await bus.PublishAsync(new SimpleMessageA { Data = id }, new MessageOptions { UniqueId = id }, timeout.Token);
        await RabbitMqReliabilityTestContext.WaitAsync(() => Volatile.Read(ref attempts) >= 3 && bus.LastDeliveryError is not null, timeout.Token);
        // Assert
        Assert.False(bus.IsSubscriptionReady);
        await bus.DisposeAsync();
        var retained = await context.ReadAsync(context.Source, timeout.Token);
        Assert.Equal(id, retained.BasicProperties.MessageId);
        Assert.Equal(3, Volatile.Read(ref attempts));
    }

    [Theory]
    [InlineData("unknown")]
    [InlineData("malformed")]
    [InlineData("unmatched")]
    public async Task SubscribeAsync_WithInvalidRequiredDelivery_TransfersToTerminalDestinationAsync(string failure)
    {
        Assert.SkipWhen(!fixture.IsAvailable, "RabbitMQ infrastructure not available");

        // Arrange
        using var timeout = CreateTimeout();
        await using var context = await RabbitMqReliabilityTestContext.CreateAsync(fixture.MessagingConnectionString!, Log, timeout.Token);
        await context.CreateDestinationAsync(timeout.Token);
        var options = context.Options();
        options.DeadLetterExchange = context.Dlx;
        options.DeadLetterRoutingKey = "quarantine";
        int handlers = 0;
        await using var bus = new RabbitMQMessageBus(options);
        await bus.SubscribeAsync<SimpleMessageA>(_ => Interlocked.Increment(ref handlers), timeout.Token);
        string id = Guid.NewGuid().ToString("N");
        string type = failure switch
        {
            "unknown" => "Unknown.Required.Event, Unknown.Required.Assembly",
            "unmatched" => typeof(SimpleMessageB).AssemblyQualifiedName!,
            _ => typeof(SimpleMessageA).AssemblyQualifiedName!
        };
        byte[] body = Encoding.UTF8.GetBytes(failure == "malformed" ? "{" : "{}");
        // Act
        await context.Admin.BasicPublishAsync(context.Topic, String.Empty, true,
            new BasicProperties { MessageId = id, Type = type, Persistent = true }, body, timeout.Token);
        var dead = await context.ReadAsync(context.Destination, timeout.Token);
        // Assert
        Assert.Equal(id, dead.BasicProperties.MessageId);
        Assert.Equal(body, dead.Body.ToArray());
        await RabbitMqReliabilityTestContext.WaitAsync(() => bus.ActiveDeliveryCount == 0, timeout.Token);
        Assert.Equal(0, Volatile.Read(ref handlers));
    }

    [Fact]
    public async Task SubscribeAsync_WithUnboundTerminalDestination_RetainsUntilRepairedAsync()
    {
        Assert.SkipWhen(!fixture.IsAvailable, "RabbitMQ infrastructure not available");

        // Arrange
        using var timeout = CreateTimeout();
        await using var context = await RabbitMqReliabilityTestContext.CreateAsync(fixture.MessagingConnectionString!, Log, timeout.Token);
        await context.Admin.ExchangeDeclareAsync(context.Dlx, "direct", true, cancellationToken: timeout.Token);
        var options = context.Options();
        options.DeliveryLimit = 0;
        options.DeadLetterExchange = context.Dlx;
        options.DeadLetterRoutingKey = "quarantine";
        int handlers = 0;
        await using var bus = new RabbitMQMessageBus(options);
        await bus.SubscribeAsync<SimpleMessageA>(_ =>
        {
            Interlocked.Increment(ref handlers);
            throw new InvalidOperationException("Synthetic terminal failure");
        }, timeout.Token);
        string id = Guid.NewGuid().ToString("N");
        // Act
        await bus.PublishAsync(new SimpleMessageA { Data = id }, new MessageOptions { UniqueId = id }, timeout.Token);
        await RabbitMqReliabilityTestContext.WaitAsync(() => bus.LastDeliveryError is not null, timeout.Token);
        // Assert
        Assert.False(bus.IsSubscriptionReady);
        await context.CreateDestinationAsync(timeout.Token);
        var dead = await context.ReadAsync(context.Destination, timeout.Token);
        Assert.Equal(id, dead.BasicProperties.MessageId);
        // Receipt by the destination can precede confirmation and the original ACK.
        // Complete the callback before graceful disposal; fault tests cover that gap separately.
        await RabbitMqReliabilityTestContext.WaitAsync(() => bus.ActiveDeliveryCount == 0, timeout.Token);
        Assert.Equal(1, Volatile.Read(ref handlers));
        await bus.DisposeAsync();
        Assert.Equal(0u, (await context.Admin.QueueDeclarePassiveAsync(context.Source, timeout.Token)).MessageCount);
    }

    private CancellationTokenSource CreateTimeout()
    {
        var timeout = CancellationTokenSource.CreateLinkedTokenSource(TestCancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(40));
        return timeout;
    }
}
