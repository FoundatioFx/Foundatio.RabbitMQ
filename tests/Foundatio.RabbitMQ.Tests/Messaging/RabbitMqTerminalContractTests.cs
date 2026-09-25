using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Foundatio.Messaging;
using Foundatio.Tests.Messaging;
using Foundatio.Xunit;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using Xunit;

namespace Foundatio.RabbitMQ.Tests.Messaging;

[Collection(nameof(RabbitMqTestCollection))]
public class RabbitMqTerminalContractTests(AspireFixture fixture, ITestOutputHelper output) : TestWithLoggingBase(output)
{
    [Fact]
    public async Task BasicRejectAsync_WithQuorumLimitAndUnavailableDestination_RetainsDeadLetterTransferAsync()
    {
        Assert.SkipWhen(!fixture.IsAvailable, "RabbitMQ infrastructure not available");

        // Arrange
        using var timeout = CreateTimeout();
        // RabbitMQ 4.2.5's dead_letter_worker_publisher_confirm_timeout defaults
        // to 180000 ms. Allow that real retry interval without changing the broker.
        // See rabbitmq-server v4.2.5, deps/rabbit/Makefile and rabbit_fifo_dlx_worker.erl.
        timeout.CancelAfter(TimeSpan.FromMinutes(4));
        var token = timeout.Token;
        await using var context = await RabbitMqReliabilityTestContext.CreateAsync(fixture.MessagingConnectionString!, Log, token);
        await context.Admin.ExchangeDeclareAsync(context.Dlx, "direct", true, cancellationToken: token);
        var options = context.Options(quorum: true);
        options.Arguments!["x-delivery-limit"] = 0L;
        options.DeliveryLimit = 0;
        options.DeadLetterExchange = context.Dlx;
        options.DeadLetterRoutingKey = "quarantine";
        options.DeadLetterStrategy = DeadLetterStrategy.AtLeastOnce;
        options.Overflow = QueueOverflowBehavior.RejectPublish;
        await using (var provision = new RabbitMQMessageBus(options))
            await provision.SubscribeAsync<SimpleMessageA>(_ => { }, token);
        string id = Guid.NewGuid().ToString("N");
        await using var publisher = new RabbitMQMessageBus(options);
        await publisher.PublishAsync(new SimpleMessageA { Data = id }, new MessageOptions { UniqueId = id }, token);
        var original = await context.ReadAsync(context.Source, token);
        Assert.Equal(id, original.BasicProperties.MessageId);
        // Act
        await context.Admin.BasicRejectAsync(original.DeliveryTag, requeue: true, token);
        // There is no provider consumer. This exercises the broker's independent limit
        // and at-least-once dead-letter worker, not the client-side terminal handoff.
        while ((await context.Admin.QueueDeclarePassiveAsync(context.Source, token)).MessageCount != 0)
            await Task.Delay(TimeSpan.FromMilliseconds(50), token);
        await Task.Delay(TimeSpan.FromSeconds(2), token);
        await context.CreateDestinationAsync(token);
        _logger.LogInformation("Repaired broker terminal route; awaiting retained transfer {MessageId}",
            id);
        var dead = await context.ReadAsync(context.Destination, token);
        // Assert
        Assert.Equal(id, dead.BasicProperties.MessageId);
        Assert.Equal(original.Body.ToArray(), dead.Body.ToArray());
        _logger.LogInformation("Broker at-least-once terminal transfer completed: {MessageId}",
            id);
    }

    [Fact]
    public async Task PublishAsync_WithRequiredDelayAndExistingFanout_RejectsMemoryFallbackAsync()
    {
        Assert.SkipWhen(String.IsNullOrEmpty(fixture.MessagingDelayedConnectionString), "Delayed broker unavailable");

        // Arrange
        using var timeout = CreateTimeout();
        var token = timeout.Token;
        await using var context = await RabbitMqReliabilityTestContext.CreateAsync(fixture.MessagingDelayedConnectionString!, Log, token);
        await context.Admin.ExchangeDeclareAsync(context.Topic, "fanout", true, cancellationToken: token);
        var options = context.Options();
        options.RequireBrokerDelayedDelivery = true;
        int received = 0;
        await using var bus = new RabbitMQMessageBus(options);
        await bus.SubscribeAsync<SimpleMessageA>(_ => Interlocked.Increment(ref received), token);
        await bus.PublishAsync(new SimpleMessageA { Data = "immediate" }, cancellationToken: token);
        await RabbitMqReliabilityTestContext.WaitAsync(() => Volatile.Read(ref received) == 1 && bus.ActiveDeliveryCount == 0, token);
        // Act
        // Assert
        await Assert.ThrowsAnyAsync<MessageBusException>(() => bus.PublishAsync(new SimpleMessageA { Data = "required-delay" },
            new MessageOptions { DeliveryDelay = TimeSpan.FromMilliseconds(100) }, token));
        await Task.Delay(TimeSpan.FromMilliseconds(300), token);
        Assert.Equal(1, Volatile.Read(ref received));
    }

    [Fact]
    public async Task PublishAsync_WithRequiredRoutingOnPluginBroker_ReportsReturnAndRecoversAsync()
    {
        Assert.SkipWhen(String.IsNullOrEmpty(fixture.MessagingDelayedConnectionString), "Delayed broker unavailable");

        // Arrange
        using var timeout = CreateTimeout();
        var token = timeout.Token;
        await using var context = await RabbitMqReliabilityTestContext.CreateAsync(fixture.MessagingDelayedConnectionString!, Log, token);
        var options = context.Options();
        options.RequirePublishRouting = true;
        var received = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var bus = new RabbitMQMessageBus(options);
        await bus.SubscribeAsync<SimpleMessageA>(message => received.TrySetResult(message.Data!), token);
        var field = typeof(RabbitMQMessageBus).GetField("_publisherChannel", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(field);
        var channel = Assert.IsAssignableFrom<IChannel>(field.GetValue(bus));
        var returned = new TaskCompletionSource<ushort>(TaskCreationOptions.RunContinuationsAsynchronously);
        channel.BasicReturnAsync += (_, args) =>
        {
            returned.TrySetResult(args.ReplyCode);
            return Task.CompletedTask;
        };
        // Act
        await context.Admin.QueueUnbindAsync(context.Source, context.Topic, String.Empty, cancellationToken: token);
        await Assert.ThrowsAnyAsync<MessageBusException>(() => bus.PublishAsync(
            new SimpleMessageA { Data = "unroutable" }, cancellationToken: token));
        // Assert
        Assert.Equal((ushort)312, await returned.Task.WaitAsync(token));
        Assert.True(channel.IsOpen);
        Assert.False(received.Task.IsCompleted);
        await context.Admin.QueueBindAsync(context.Source, context.Topic, String.Empty, cancellationToken: token);
        await bus.PublishAsync(new SimpleMessageA { Data = "routed" }, cancellationToken: token);
        Assert.Equal("routed", await received.Task.WaitAsync(token));
        await RabbitMqReliabilityTestContext.WaitAsync(() => bus.ActiveDeliveryCount == 0, token);
    }

    [Fact]
    public async Task SubscribeAsync_WithExplicitDiscardAndNoDestination_DiscardsExhaustedDeliveryAsync()
    {
        Assert.SkipWhen(!fixture.IsAvailable, "RabbitMQ infrastructure not available");

        // Arrange
        using var timeout = CreateTimeout();
        var token = timeout.Token;
        await using var context = await RabbitMqReliabilityTestContext.CreateAsync(fixture.MessagingConnectionString!, Log, token);
        var options = context.Options();
        options.RequireSuccessfulDispatch = false;
        options.DeadLetterExchange = null;
        options.DiscardOnDeliveryLimit = true;
        options.DeliveryLimit = 0;
        int calls = 0;
        await using var bus = new RabbitMQMessageBus(options);
        await bus.SubscribeAsync<SimpleMessageA>(_ =>
        {
            Interlocked.Increment(ref calls);
            throw new InvalidOperationException("Explicitly discardable test message");
        }, token);
        // Act
        await bus.PublishAsync(new SimpleMessageA { Data = "discardable" }, cancellationToken: token);
        await RabbitMqReliabilityTestContext.WaitAsync(() => Volatile.Read(ref calls) == 1 && bus.ActiveDeliveryCount == 0, token);
        await bus.DisposeAsync();
        // Assert
        Assert.Null(await context.Admin.BasicGetAsync(context.Source, false, token));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SubscribeAsync_WithInvalidOrExceededRetryMetadata_DoesNotResetBudgetAsync(bool malformed)
    {
        Assert.SkipWhen(!fixture.IsAvailable, "RabbitMQ infrastructure not available");

        // Arrange
        using var timeout = CreateTimeout();
        var token = timeout.Token;
        await using var context = await RabbitMqReliabilityTestContext.CreateAsync(fixture.MessagingConnectionString!, Log, token);
        await context.CreateDestinationAsync(token);
        var options = context.Options();
        options.DeadLetterExchange = context.Dlx;
        options.DeadLetterRoutingKey = "quarantine";
        int calls = 0;
        await using var bus = new RabbitMQMessageBus(options);
        await bus.SubscribeAsync<SimpleMessageA>(_ =>
        {
            Interlocked.Increment(ref calls);
            throw new InvalidOperationException("Synthetic exhausted message");
        }, token);
        string id = Guid.NewGuid().ToString("N");
        var properties = new BasicProperties
        {
            MessageId = id,
            Persistent = true,
            Type = typeof(SimpleMessageA).AssemblyQualifiedName,
            Headers = new Dictionary<string, object?> { ["x-delivery-count"] = malformed ? "invalid" : (object)Int64.MaxValue }
        };
        // Act
        await context.Admin.BasicPublishAsync(context.Topic, String.Empty, true, properties,
            JsonSerializer.SerializeToUtf8Bytes(new SimpleMessageA { Data = id }), token);
        var dead = await context.ReadAsync(context.Destination, token);
        // Assert
        Assert.Equal(id, dead.BasicProperties.MessageId);
        await RabbitMqReliabilityTestContext.WaitAsync(() => bus.ActiveDeliveryCount == 0, token);
        Assert.Equal(1, Volatile.Read(ref calls));
    }

    private CancellationTokenSource CreateTimeout()
    {
        var timeout = CancellationTokenSource.CreateLinkedTokenSource(TestCancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(90));
        return timeout;
    }
}
