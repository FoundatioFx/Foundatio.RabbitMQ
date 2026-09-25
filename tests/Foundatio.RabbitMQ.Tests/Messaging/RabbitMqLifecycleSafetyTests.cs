using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Foundatio.AsyncEx;
using Foundatio.Messaging;
using Foundatio.Tests.Messaging;
using Foundatio.Xunit;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;
using Xunit;

namespace Foundatio.RabbitMQ.Tests.Messaging;

[Collection(nameof(RabbitMqTestCollection))]
public class RabbitMqLifecycleSafetyTests(AspireFixture fixture, ITestOutputHelper output) : TestWithLoggingBase(output)
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task DisposeAsync_WithUncooperativeHandler_RetainsDeliveryAndOwnedBodyAsync(bool quorum)
    {
        Assert.SkipWhen(!fixture.IsAvailable, "RabbitMQ infrastructure not available");

        // Arrange
        using var timeout = CreateTimeout();
        var token = timeout.Token;
        await using var context = await RabbitMqReliabilityTestContext.CreateAsync(fixture.MessagingConnectionString!, Log, token);
        await using var bus = new RabbitMQMessageBus(context.Options(quorum));
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var lateBody = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        string id = Guid.NewGuid().ToString("N");
        try
        {
            await bus.SubscribeAsync<IMessage>(async message =>
            {
                entered.TrySetResult();
                // Deliberately ignore cancellation to prove transport cleanup is not
                // dependent on cooperative application code or pooled payload lifetime.
                await release.Task;
                lateBody.TrySetResult(Encoding.UTF8.GetString(message.Data.Span));
            }, token);
            await bus.PublishAsync(new SimpleMessageA { Data = id }, new MessageOptions { UniqueId = id }, token);
            await entered.Task.WaitAsync(token);
            // Act
            await bus.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10), token);
            // Assert
            Assert.False(bus.IsSubscriptionReady);
            var retained = await context.ReadAsync(context.Source, token);
            Assert.Equal(id, retained.BasicProperties.MessageId);
            release.TrySetResult();
            Assert.Contains(id, await lateBody.Task.WaitAsync(token));
        }
        finally
        {
            release.TrySetResult();
        }
    }

    [Theory]
    [InlineData(AcknowledgementStrategy.FireAndForget)]
    [InlineData(AcknowledgementStrategy.Automatic)]
    public async Task SubscribeAsync_AfterLastSubscriptionCancellation_StopsConsumerAndResubscribesAsync(
        AcknowledgementStrategy acknowledgementStrategy)
    {
        Assert.SkipWhen(!fixture.IsAvailable, "RabbitMQ infrastructure not available");

        // Arrange
        using var timeout = CreateTimeout();
        var token = timeout.Token;
        using var subscription = CancellationTokenSource.CreateLinkedTokenSource(token);
        await using var context = await RabbitMqReliabilityTestContext.CreateAsync(fixture.MessagingConnectionString!, Log, token);
        var options = context.Options();
        options.AcknowledgementStrategy = acknowledgementStrategy;
        options.RequireSuccessfulDispatch = false;
        await using var bus = new RabbitMQMessageBus(options);
        int cancelledCalls = 0;
        await bus.SubscribeAsync<SimpleMessageA>(_ => Interlocked.Increment(ref cancelledCalls), subscription.Token);

        // Act
        await subscription.CancelAsync();
        while ((await context.Admin.QueueDeclarePassiveAsync(context.Source, token)).ConsumerCount != 0)
            await Task.Delay(TimeSpan.FromMilliseconds(50), token);
        // Assert
        Assert.False(bus.IsSubscriptionReady);
        string id = Guid.NewGuid().ToString("N");
        await bus.PublishAsync(new SimpleMessageA { Data = id }, new MessageOptions { UniqueId = id }, token);
        Assert.Equal(1u, (await context.Admin.QueueDeclarePassiveAsync(context.Source, token)).MessageCount);
        var received = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        await bus.SubscribeAsync<SimpleMessageA>(message => received.TrySetResult(message.Data!), token);
        Assert.Equal(id, await received.Task.WaitAsync(token));
        await RabbitMqReliabilityTestContext.WaitAsync(() => bus.ActiveDeliveryCount == 0, token);
        Assert.Equal(0, Volatile.Read(ref cancelledCalls));
        Assert.True(bus.IsSubscriptionReady);
    }

    [Fact]
    public async Task SubscribeAsync_AfterQueueDeletion_RecreatesConsumerForNewWorkAsync()
    {
        Assert.SkipWhen(!fixture.IsAvailable, "RabbitMQ infrastructure not available");

        // Arrange
        using var timeout = CreateTimeout();
        var token = timeout.Token;
        await using var context = await RabbitMqReliabilityTestContext.CreateAsync(fixture.MessagingConnectionString!, Log, token);
        await using var bus = new RabbitMQMessageBus(context.Options());
        var receipts = new ConcurrentDictionary<string, byte>();
        await bus.SubscribeAsync<SimpleMessageA>(message => receipts.TryAdd(message.Data!, 0), token);
        await bus.PublishAsync(new SimpleMessageA { Data = "before" }, cancellationToken: token);
        await RabbitMqReliabilityTestContext.WaitAsync(() => receipts.ContainsKey("before") && bus.ActiveDeliveryCount == 0, token);
        var oldConsumer = GetField<AsyncEventingBasicConsumer>(bus, "_consumer");
        // Act
        await context.Admin.QueueDeleteAsync(context.Source, cancellationToken: token);
        await RabbitMqReliabilityTestContext.WaitAsync(() => bus.IsSubscriptionReady
            && !ReferenceEquals(oldConsumer, GetField<AsyncEventingBasicConsumer>(bus, "_consumer")), token);
        await bus.PublishAsync(new SimpleMessageA { Data = "after" }, cancellationToken: token);
        await RabbitMqReliabilityTestContext.WaitAsync(() => receipts.ContainsKey("after") && bus.ActiveDeliveryCount == 0, token);
        // Assert
        Assert.Equal(2, receipts.Count);
        // Administrative deletion destroys queued work. Only post-recreation progress is claimed.
    }

    [Theory]
    [InlineData(AcknowledgementStrategy.FireAndForget)]
    [InlineData(AcknowledgementStrategy.Automatic)]
    public async Task SubscribeAsync_DuringConsumerCleanup_WaitsForReplacementConsumerAsync(AcknowledgementStrategy strategy)
    {
        Assert.SkipWhen(!fixture.IsAvailable, "RabbitMQ infrastructure not available");

        // Arrange
        using var timeout = CreateTimeout();
        var token = timeout.Token;
        using var first = CancellationTokenSource.CreateLinkedTokenSource(token);
        await using var context = await RabbitMqReliabilityTestContext.CreateAsync(fixture.MessagingConnectionString!, Log, token);
        var options = context.Options();
        options.RequireSuccessfulDispatch = false;
        options.AcknowledgementStrategy = strategy;
        await using var bus = new RabbitMQMessageBus(options);
        await bus.SubscribeAsync<SimpleMessageA>(_ => { }, first.Token);
        var received = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        Task subscribing;

        // Act
        using (await GetField<AsyncLock>(bus, "_subscriberLock").LockAsync(token))
        {
            await first.CancelAsync();
            subscribing = bus.SubscribeAsync<SimpleMessageA>(message => received.TrySetResult(message.Data!), token);
            try
            {
                Assert.False(subscribing.IsCompleted, "Subscription must wait for in-progress consumer cleanup.");
            }
            finally
            {
                var clear = typeof(RabbitMQMessageBus).GetMethod("ClearSubscriberChannelAsync", BindingFlags.Instance | BindingFlags.NonPublic);
                Assert.NotNull(clear);
                await Assert.IsAssignableFrom<Task>(clear.Invoke(bus, null));
            }
        }
        await subscribing.WaitAsync(token);
        await bus.PublishAsync(new SimpleMessageA { Data = "replacement" }, cancellationToken: token);

        // Assert
        Assert.Equal("replacement", await received.Task.WaitAsync(token));
        Assert.True(bus.IsSubscriptionReady);
        Assert.Equal(1u, (await context.Admin.QueueDeclarePassiveAsync(context.Source, token)).ConsumerCount);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SubscribeAsync_RepeatedStartup_KeepsRegisteredConsumerAsync(bool quorum)
    {
        Assert.SkipWhen(!fixture.IsAvailable, "RabbitMQ infrastructure not available");

        // Arrange
        using var timeout = CreateTimeout();
        var token = timeout.Token;
        await using var context = await RabbitMqReliabilityTestContext.CreateAsync(fixture.MessagingConnectionString!, Log, token);

        for (int iteration = 0; iteration < 50; iteration++)
        {
            await using var bus = new RabbitMQMessageBus(context.Options(quorum));

            // Act
            await bus.SubscribeAsync<SimpleMessageA>(_ => { }, token);
            var consumer = GetField<AsyncEventingBasicConsumer>(bus, "_consumer");
            bool ready = bus.IsSubscriptionReady;
            await bus.SubscribeAsync<SimpleMessageB>(_ => { }, token);

            // Assert
            Assert.True(ready);
            Assert.Same(consumer, GetField<AsyncEventingBasicConsumer>(bus, "_consumer"));
            Assert.True(bus.IsSubscriptionReady);
        }
    }

    [Fact]
    public async Task SubscribeAsync_WhenOnlyCallerCancelsDuringSetup_DoesNotLeaveOrphanConsumerAsync()
    {
        Assert.SkipWhen(!fixture.IsAvailable, "RabbitMQ infrastructure not available");

        // Arrange
        using var timeout = CreateTimeout();
        var token = timeout.Token;
        using var subscription = CancellationTokenSource.CreateLinkedTokenSource(token);
        await using var context = await RabbitMqReliabilityTestContext.CreateAsync(fixture.MessagingConnectionString!, Log, token);
        var options = context.Options();
        await context.Admin.QueueDeclareAsync(context.Source, true, false, false, options.Arguments, cancellationToken: token);
        await using var bus = new GatedInitializationBus(options);
        Task subscribing = bus.SubscribeAsync<SimpleMessageA>(_ => { }, subscription.Token);
        try
        {
            await bus.InitializationStarted.Task.WaitAsync(token);
            // Act
            await subscription.CancelAsync();
            bus.ReleaseInitialization();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => subscribing.WaitAsync(token));

            while ((await context.Admin.QueueDeclarePassiveAsync(context.Source, token)).ConsumerCount != 0)
                await Task.Delay(TimeSpan.FromMilliseconds(50), token);
            // Assert
            Assert.False(bus.IsSubscriptionReady);
        }
        finally
        {
            bus.ReleaseInitialization();
            await Record.ExceptionAsync(async () => await subscribing.WaitAsync(token));
        }
    }

    [Fact]
    public async Task SubscribeAsync_WithCancelledSetupCaller_PreservesAnotherPendingSubscriptionAsync()
    {
        Assert.SkipWhen(!fixture.IsAvailable, "RabbitMQ infrastructure not available");

        // Arrange
        using var timeout = CreateTimeout();
        var token = timeout.Token;
        using var cancelledSubscription = CancellationTokenSource.CreateLinkedTokenSource(token);
        await using var context = await RabbitMqReliabilityTestContext.CreateAsync(fixture.MessagingConnectionString!, Log, token);
        await using var bus = new GatedInitializationBus(context.Options());
        int cancelledCalls = 0;
        var received = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        Task first = bus.SubscribeAsync<SimpleMessageA>(_ => Interlocked.Increment(ref cancelledCalls), cancelledSubscription.Token);
        Task? second = null;
        try
        {
            await bus.InitializationStarted.Task.WaitAsync(token);
            second = bus.SubscribeAsync<SimpleMessageA>(message => received.TrySetResult(message.Data!), token);
            // Act
            await cancelledSubscription.CancelAsync();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => first.WaitAsync(TimeSpan.FromSeconds(5), token));
            // Assert
            Assert.False(second.IsCompleted);
            bus.ReleaseInitialization();
            await second.WaitAsync(token);
            string id = Guid.NewGuid().ToString("N");
            await bus.PublishAsync(new SimpleMessageA { Data = id }, new MessageOptions { UniqueId = id }, token);
            Assert.Equal(id, await received.Task.WaitAsync(token));
            await RabbitMqReliabilityTestContext.WaitAsync(() => bus.ActiveDeliveryCount == 0, token);
            Assert.Equal(0, Volatile.Read(ref cancelledCalls));
            Assert.True(bus.IsSubscriptionReady);
            Assert.Equal(1u, (await context.Admin.QueueDeclarePassiveAsync(context.Source, token)).ConsumerCount);
        }
        finally
        {
            bus.ReleaseInitialization();
            await Record.ExceptionAsync(async () => await first.WaitAsync(token));
            if (second is not null)
                await Record.ExceptionAsync(async () => await second.WaitAsync(token));
        }
    }

    [Fact]
    public async Task SubscribeAsync_WithFailedInitialization_RemovesRegistrationAndAllowsRetryAsync()
    {
        Assert.SkipWhen(!fixture.IsAvailable, "RabbitMQ infrastructure not available");

        // Arrange
        using var timeout = CreateTimeout();
        var token = timeout.Token;
        await using var context = await RabbitMqReliabilityTestContext.CreateAsync(fixture.MessagingConnectionString!, Log, token);
        await context.Admin.QueueDeclareAsync(context.Source, true, false, false,
            new Dictionary<string, object?> { ["x-queue-type"] = "classic" }, cancellationToken: token);
        await using var bus = new RabbitMQMessageBus(context.Options(quorum: true));
        int failedRegistrationCalls = 0;
        // Act
        var error = await Assert.ThrowsAnyAsync<OperationInterruptedException>(() => bus.SubscribeAsync<SimpleMessageA>(
            _ => Interlocked.Increment(ref failedRegistrationCalls), token));
        // Assert
        Assert.Equal((ushort)406, error.ShutdownReason!.ReplyCode);
        Assert.False(bus.IsSubscriptionReady);
        Assert.NotNull(bus.LastSubscriptionError);

        await context.Admin.QueueDeleteAsync(context.Source, cancellationToken: token);
        int correctCalls = 0;
        await bus.SubscribeAsync<SimpleMessageA>(_ => Interlocked.Increment(ref correctCalls), token);
        await bus.PublishAsync(new SimpleMessageA { Data = context.Topic }, cancellationToken: token);
        await RabbitMqReliabilityTestContext.WaitAsync(() => Volatile.Read(ref correctCalls) == 1 && bus.ActiveDeliveryCount == 0, token);
        Assert.Equal(0, Volatile.Read(ref failedRegistrationCalls));
        Assert.True(bus.IsSubscriptionReady);
        Assert.Null(bus.LastSubscriptionError);
    }

    [Theory]
    [InlineData(AcknowledgementStrategy.FireAndForget, false, false)]
    [InlineData(AcknowledgementStrategy.Automatic, false, false)]
    [InlineData(AcknowledgementStrategy.Automatic, true, false)]
    [InlineData(AcknowledgementStrategy.Automatic, true, true)]
    public async Task SubscribeAsync_WithHandlerCancellation_UsesRetryPolicyAndContinuesAsync(
        AcknowledgementStrategy acknowledgementStrategy, bool requireSuccessfulDispatch, bool quorum)
    {
        Assert.SkipWhen(!fixture.IsAvailable, "RabbitMQ infrastructure not available");

        // Arrange
        using var timeout = CreateTimeout();
        var token = timeout.Token;
        await using var context = await RabbitMqReliabilityTestContext.CreateAsync(fixture.MessagingConnectionString!, Log, token);
        var options = context.Options(quorum);
        options.AcknowledgementStrategy = acknowledgementStrategy;
        options.RequireSuccessfulDispatch = requireSuccessfulDispatch;
        await using var bus = new RabbitMQMessageBus(options);
        string cancelledId = Guid.NewGuid().ToString("N");
        string nextId = Guid.NewGuid().ToString("N");
        int attempts = 0;
        var completed = new ConcurrentDictionary<string, int>();
        await bus.SubscribeAsync<SimpleMessageA>(message =>
        {
            if (message.Data == cancelledId && Interlocked.Increment(ref attempts) == 1)
                throw new OperationCanceledException("Synthetic handler-local cancellation");
            completed.AddOrUpdate(message.Data!, 1, (_, count) => count + 1);
        }, token);

        // Act
        await bus.PublishAsync(new SimpleMessageA { Data = cancelledId }, new MessageOptions { UniqueId = cancelledId }, token);
        await bus.PublishAsync(new SimpleMessageA { Data = nextId }, new MessageOptions { UniqueId = nextId }, token);
        bool retries = acknowledgementStrategy == AcknowledgementStrategy.Automatic;
        await RabbitMqReliabilityTestContext.WaitAsync(() => completed.ContainsKey(nextId)
            && (!retries || completed.ContainsKey(cancelledId)) && bus.ActiveDeliveryCount == 0, token);

        // Assert
        Assert.False(token.IsCancellationRequested);
        Assert.Equal(retries ? 2 : 1, Volatile.Read(ref attempts));
        Assert.Equal(retries ? 2 : 1, completed.Count);
        Assert.Equal(1, completed[nextId]);
        if (retries)
            Assert.Equal(1, completed[cancelledId]);
        Assert.Null(bus.LastDeliveryError);
        Assert.True(bus.IsSubscriptionReady);
        Assert.Equal(0u, (await context.Admin.QueueDeclarePassiveAsync(context.Source, token)).MessageCount);
    }

    [Fact]
    public async Task SubscribeAsync_WithLateHandlerCompletion_DoesNotSettleReplacementDeliveryAsync()
    {
        Assert.SkipWhen(!fixture.IsAvailable, "RabbitMQ infrastructure not available");

        // Arrange
        using var timeout = CreateTimeout();
        var token = timeout.Token;
        await using var context = await RabbitMqReliabilityTestContext.CreateAsync(fixture.MessagingConnectionString!, Log, token);
        await using var bus = new RabbitMQMessageBus(context.Options());
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var lateCompleted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var receipts = new ConcurrentDictionary<string, int>();
        int attempts = 0;
        try
        {
            await bus.SubscribeAsync<SimpleMessageA>(async message =>
            {
                if (Interlocked.Increment(ref attempts) == 1)
                {
                    entered.TrySetResult();
                    await release.Task;
                    lateCompleted.TrySetResult();
                    return;
                }
                receipts.AddOrUpdate(message.Data!, 1, (_, count) => count + 1);
            }, token);
            await bus.PublishAsync(new SimpleMessageA { Data = "pending" }, cancellationToken: token);
            await entered.Task.WaitAsync(token);
            var connection = GetField<IConnection>(bus, "_subscriberConnection");
            var oldChannel = GetField<IChannel>(bus, "_subscriberChannel");
            // Act
            await oldChannel.CloseAsync(200, "Synthetic channel replacement", cancellationToken: token).WaitAsync(token);
            // Assert
            Assert.True(connection.IsOpen);
            await RabbitMqReliabilityTestContext.WaitAsync(() => receipts.ContainsKey("pending") && bus.IsSubscriptionReady && bus.ActiveDeliveryCount == 0, token);
            var current = GetField<IChannel>(bus, "_subscriberChannel");
            Assert.NotSame(oldChannel, current);
            release.TrySetResult();
            await lateCompleted.Task.WaitAsync(token);
            await bus.PublishAsync(new SimpleMessageA { Data = "after" }, cancellationToken: token);
            await RabbitMqReliabilityTestContext.WaitAsync(() => receipts.ContainsKey("after") && bus.ActiveDeliveryCount == 0, token);
            Assert.True(current.IsOpen);
            Assert.Same(current, GetField<IChannel>(bus, "_subscriberChannel"));
            Assert.Equal(1, receipts["pending"]);
            Assert.Equal(1, receipts["after"]);
        }
        finally
        {
            release.TrySetResult();
        }
    }

    [Fact]
    public async Task SubscribeAsync_WithOneCancelledHandler_DispatchesToRemainingRequiredHandlerAsync()
    {
        Assert.SkipWhen(!fixture.IsAvailable, "RabbitMQ infrastructure not available");

        // Arrange
        using var timeout = CreateTimeout();
        var token = timeout.Token;
        using var subscription = CancellationTokenSource.CreateLinkedTokenSource(token);
        await using var context = await RabbitMqReliabilityTestContext.CreateAsync(fixture.MessagingConnectionString!, Log, token);
        await using var bus = new RabbitMQMessageBus(context.Options());
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await bus.SubscribeAsync<SimpleMessageA>(async (_, handlerToken) =>
        {
            entered.TrySetResult();
            await Task.Delay(Timeout.InfiniteTimeSpan, handlerToken);
        }, subscription.Token);
        int completed = 0;
        await bus.SubscribeAsync<SimpleMessageA>(_ => Interlocked.Increment(ref completed), token);
        await bus.PublishAsync(new SimpleMessageA { Data = "first" }, cancellationToken: token);
        await entered.Task.WaitAsync(token);
        // Act
        await subscription.CancelAsync();
        await RabbitMqReliabilityTestContext.WaitAsync(() => Volatile.Read(ref completed) == 1 && bus.ActiveDeliveryCount == 0, token);
        await bus.PublishAsync(new SimpleMessageA { Data = "second" }, cancellationToken: token);
        await RabbitMqReliabilityTestContext.WaitAsync(() => Volatile.Read(ref completed) == 2 && bus.ActiveDeliveryCount == 0, token);
        // Assert
        Assert.True(bus.IsSubscriptionReady);
        Assert.Equal(0u, (await context.Admin.QueueDeclarePassiveAsync(context.Source, token)).MessageCount);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SubscribeAsync_WithRepeatedHandlerCancellation_ReachesTerminalDestinationAsync(bool quorum)
    {
        Assert.SkipWhen(!fixture.IsAvailable, "RabbitMQ infrastructure not available");

        // Arrange
        using var timeout = CreateTimeout();
        var token = timeout.Token;
        await using var context = await RabbitMqReliabilityTestContext.CreateAsync(fixture.MessagingConnectionString!, Log, token);
        await context.CreateDestinationAsync(token);
        var options = context.Options(quorum);
        options.DeadLetterExchange = context.Dlx;
        options.DeadLetterRoutingKey = "quarantine";
        await using var bus = new RabbitMQMessageBus(options);
        int attempts = 0;
        await bus.SubscribeAsync<SimpleMessageA>(_ =>
        {
            Interlocked.Increment(ref attempts);
            throw new OperationCanceledException("Synthetic handler-local cancellation");
        }, token);
        string id = Guid.NewGuid().ToString("N");

        // Act
        await bus.PublishAsync(new SimpleMessageA { Data = id }, new MessageOptions { UniqueId = id }, token);
        var terminal = await context.ReadAsync(context.Destination, token);
        await RabbitMqReliabilityTestContext.WaitAsync(() => bus.ActiveDeliveryCount == 0, token);

        // Assert
        Assert.Equal(id, terminal.BasicProperties.MessageId);
        Assert.Contains(id, Encoding.UTF8.GetString(terminal.Body.Span));
        Assert.Equal(2, Volatile.Read(ref attempts));
        Assert.True(bus.IsSubscriptionReady);
        Assert.Equal(0u, (await context.Admin.QueueDeclarePassiveAsync(context.Source, token)).MessageCount);
    }

    private CancellationTokenSource CreateTimeout()
    {
        var timeout = CancellationTokenSource.CreateLinkedTokenSource(TestCancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(40));
        return timeout;
    }

    private static T GetField<T>(RabbitMQMessageBus bus, string name)
    {
        var field = typeof(RabbitMQMessageBus).GetField(name, BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(field);
        return Assert.IsAssignableFrom<T>(field.GetValue(bus));
    }

    private sealed class GatedInitializationBus(RabbitMQMessageBusOptions options) : RabbitMQMessageBus(options)
    {
        private readonly TaskCompletionSource _releaseInitialization = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal TaskCompletionSource InitializationStarted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal void ReleaseInitialization() => _releaseInitialization.TrySetResult();

        protected override async Task EnsureTopicCreatedAsync(CancellationToken cancellationToken)
        {
            InitializationStarted.TrySetResult();
            await _releaseInitialization.Task.WaitAsync(cancellationToken);
            await base.EnsureTopicCreatedAsync(cancellationToken);
        }
    }
}
