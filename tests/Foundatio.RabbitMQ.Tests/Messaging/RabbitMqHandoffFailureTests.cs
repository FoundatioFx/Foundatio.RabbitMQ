using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Aspire.Hosting.Testing;
using Foundatio.Messaging;
using Foundatio.Tests.Messaging;
using Foundatio.Xunit;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using Xunit;

namespace Foundatio.RabbitMQ.Tests.Messaging;

[Collection(nameof(RabbitMqTestCollection))]
public class RabbitMqHandoffFailureTests(AspireFixture fixture, ITestOutputHelper output) : TestWithLoggingBase(output)
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SubscribeAsync_AfterRetryHandoffRecovery_PreservesIdentityWithoutRebroadcastAsync(bool publicationWasAccepted)
    {
        Assert.SkipWhen(!fixture.IsAvailable, "RabbitMQ infrastructure not available");

        // Arrange
        using var timeout = CreateTimeout();
        var token = timeout.Token;
        await using var context = await RabbitMqReliabilityTestContext.CreateAsync(fixture.MessagingConnectionString!, Log, token);
        await using var healthy = new RabbitMQMessageBus(context.Options(healthy: true));
        var healthyIds = new ConcurrentQueue<string>();
        await healthy.SubscribeAsync<IMessage<SimpleMessageA>>(message => healthyIds.Enqueue(message.UniqueId!), token);
        var options = context.Options();
        // Normal producer confirms remain off; the retry channel must still require them.
        options.PublisherConfirmsEnabled = false;
        await using var failed = new FaultedHandoffBus(options, publicationWasAccepted);
        var deliveryIds = new ConcurrentQueue<string>();
        int attempts = 0;
        await failed.SubscribeAsync<IMessage<SimpleMessageA>>(message =>
        {
            deliveryIds.Enqueue(message.UniqueId!);
            if (Interlocked.Increment(ref attempts) == 1)
                throw new InvalidOperationException("Synthetic first attempt failure");
        }, token);
        string id = Guid.NewGuid().ToString("N");
        // Act
        await failed.PublishAsync(new SimpleMessageA { Data = id }, new MessageOptions { UniqueId = id }, token);
        await failed.FirstFailedAttempt.Task.WaitAsync(token);
        await RabbitMqReliabilityTestContext.WaitAsync(() => failed.LastDeliveryError is not null, token);
        // Assert
        Assert.True(GetSubscriberConnection(failed).IsOpen);
        failed.AllowRecovery();
        int expectedAttempts = publicationWasAccepted ? 3 : 2;
        await RabbitMqReliabilityTestContext.WaitAsync(() => Volatile.Read(ref attempts) == expectedAttempts
            && failed.ActiveDeliveryCount == 0 && healthyIds.Count == 1 && healthy.ActiveDeliveryCount == 0, token);
        Assert.Equal(Enumerable.Repeat(id, expectedAttempts), deliveryIds.ToArray());
        Assert.Equal(new[] { id }, healthyIds.ToArray());
        Assert.Equal(0u, (await context.Admin.QueueDeclarePassiveAsync(context.Source, token)).MessageCount);
        Assert.True(failed.IsSubscriptionReady);
        _logger.LogInformation("Recovered accepted={PublicationWasAccepted}; attempts={ExpectedAttempts}; independent deliveries=1; stableId={MessageId}",
            publicationWasAccepted, expectedAttempts, id);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SubscribeAsync_WithDifferentPublisherIdentity_TransfersOnlyToQuarantineAsync(bool quorum)
    {
        Assert.SkipWhen(!fixture.IsAvailable, "RabbitMQ infrastructure not available");

        // Arrange
        using var timeout = CreateTimeout();
        var token = timeout.Token;
        await using var context = await RabbitMqReliabilityTestContext.CreateAsync(fixture.MessagingConnectionString!, Log, token);
        await context.CreateDestinationAsync(token);
        await context.Admin.QueueDeclareAsync(context.Healthy, true, false, false, cancellationToken: token);
        await context.Admin.QueueBindAsync(context.Healthy, context.Dlx, "unintended", cancellationToken: token);
        var originalUri = new Uri(fixture.MessagingConnectionString!);
        using var management = fixture.App.CreateHttpClient("messaging", "management");
        management.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic",
            Convert.ToBase64String(Encoding.UTF8.GetBytes(Uri.UnescapeDataString(originalUri.UserInfo))));
        string user = context.Topic;
        string password = Guid.NewGuid().ToString("N");
        using var created = await management.PutAsJsonAsync($"api/users/{user}", new { password, tags = "" }, token);
        created.EnsureSuccessStatusCode();
        try
        {
            using var permissions = await management.PutAsJsonAsync($"api/permissions/%2F/{user}", new
            {
                configure = $"^{context.Topic}.*$",
                read = $"^{context.Topic}.*$",
                write = $"^(amq.default|{context.Topic}.*)$"
            }, token);
            permissions.EnsureSuccessStatusCode();
            var options = context.Options(quorum);
            options.ConnectionString = new UriBuilder(originalUri) { UserName = user, Password = password }.Uri.AbsoluteUri;
            options.DeliveryLimit = 0;
            await using var bus = new RabbitMQMessageBus(options);
            await bus.SubscribeAsync<IMessage>(_ => throw new InvalidOperationException("Synthetic terminal failure"), token);
            var properties = new BasicProperties
            {
                UserId = Uri.UnescapeDataString(originalUri.UserInfo.Split(':')[0]),
                MessageId = "cross-account",
                Persistent = true,
                Headers = new Dictionary<string, object?> { ["CC"] = new object[] { "unintended" }, ["application-header"] = "keep" }
            };

            // Act
            await context.Admin.BasicPublishAsync(context.Topic, String.Empty, true, properties, Encoding.UTF8.GetBytes("payload"), token);
            var terminal = await context.ReadAsync(context.Destination, token);
            await RabbitMqReliabilityTestContext.WaitAsync(() => bus.ActiveDeliveryCount == 0, token);

            // Assert
            Assert.Null(terminal.BasicProperties.UserId);
            Assert.Equal("cross-account", terminal.BasicProperties.MessageId);
            Assert.Equal("payload", Encoding.UTF8.GetString(terminal.Body.Span));
            Assert.DoesNotContain("CC", terminal.BasicProperties.Headers!);
            Assert.Contains("application-header", terminal.BasicProperties.Headers!);
            Assert.Equal(0u, (await context.Admin.QueueDeclarePassiveAsync(context.Healthy, token)).MessageCount);
            Assert.True(bus.IsSubscriptionReady);
        }
        finally
        {
            using var cleanup = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            using var deleted = await management.DeleteAsync($"api/users/{user}", cleanup.Token);
            deleted.EnsureSuccessStatusCode();
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SubscribeAsync_WithFailedRetryHandoff_RetainsOriginalOnHealthySubscriberAsync(bool publicationWasAccepted)
    {
        Assert.SkipWhen(!fixture.IsAvailable, "RabbitMQ infrastructure not available");

        // Arrange
        using var timeout = CreateTimeout();
        var token = timeout.Token;
        await using var context = await RabbitMqReliabilityTestContext.CreateAsync(fixture.MessagingConnectionString!, Log, token);
        var options = context.Options();
        options.PublisherConfirmsEnabled = false;
        await using var bus = new FaultedHandoffBus(options, publicationWasAccepted);
        await bus.SubscribeAsync<SimpleMessageA>(_ => throw new InvalidOperationException("Synthetic handler failure"), token);
        string id = Guid.NewGuid().ToString("N");
        // Act
        await bus.PublishAsync(new SimpleMessageA { Data = id }, new MessageOptions { UniqueId = id }, token);
        await bus.FirstFailedAttempt.Task.WaitAsync(token);
        await RabbitMqReliabilityTestContext.WaitAsync(() => bus.LastDeliveryError is not null, token);
        // Assert
        Assert.True(GetSubscriberConnection(bus).IsOpen, "The original subscriber must still be capable of acknowledging.");
        Assert.False(bus.IsSubscriptionReady);
        Assert.Equal(publicationWasAccepted ? 1u : 0u, (await context.Admin.QueueDeclarePassiveAsync(context.Source, token)).MessageCount);

        await bus.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10), token);
        int expectedCopies = publicationWasAccepted ? 2 : 1;
        for (int i = 0; i < expectedCopies; i++)
        {
            var retained = await context.ReadAsync(context.Source, token);
            Assert.Equal(id, retained.BasicProperties.MessageId);
            Assert.Contains(id, Encoding.UTF8.GetString(retained.Body.Span));
        }
        Assert.Null(await context.Admin.BasicGetAsync(context.Source, false, token));
        _logger.LogInformation("Injected accepted={PublicationWasAccepted}; subscriber stayed open; retainedCopies={ExpectedCopies}; id={MessageId}",
            publicationWasAccepted, expectedCopies, id);
    }

    [Fact]
    public async Task SubscribeAsync_WithFullClassicRetryQueue_RetainsUntilCapacityReturnsAsync()
    {
        Assert.SkipWhen(!fixture.IsAvailable, "RabbitMQ infrastructure not available");

        // Arrange
        using var timeout = CreateTimeout();
        var token = timeout.Token;
        await using var context = await RabbitMqReliabilityTestContext.CreateAsync(fixture.MessagingConnectionString!, Log, token);
        var options = context.Options();
        options.Arguments!["x-max-length"] = 1L;
        options.Overflow = QueueOverflowBehavior.RejectPublish;
        await using var bus = new RabbitMQMessageBus(options);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var fail = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        int attempts = 0;
        await bus.SubscribeAsync<SimpleMessageA>(async _ =>
        {
            if (Interlocked.Increment(ref attempts) == 1)
            {
                entered.TrySetResult();
                await fail.Task.WaitAsync(token);
                throw new InvalidOperationException("Synthetic retry after source fills");
            }
        }, token);

        // Act
        await bus.PublishAsync(new SimpleMessageA { Data = "original" }, new MessageOptions { UniqueId = "original" }, token);
        await entered.Task.WaitAsync(token);
        await bus.PublishAsync(new SimpleMessageA { Data = "filler" }, new MessageOptions { UniqueId = "filler" }, token);
        fail.TrySetResult();
        await RabbitMqReliabilityTestContext.WaitAsync(() => bus.LastDeliveryError is not null, token);

        // Assert
        Assert.False(bus.IsSubscriptionReady);
        Assert.Equal(1, Volatile.Read(ref attempts));
        var filler = await context.Admin.BasicGetAsync(context.Source, true, token);
        Assert.NotNull(filler);
        Assert.Equal("filler", filler.BasicProperties.MessageId);
        await RabbitMqReliabilityTestContext.WaitAsync(() => Volatile.Read(ref attempts) == 2 && bus.ActiveDeliveryCount == 0, token);
        Assert.True(bus.IsSubscriptionReady);
        Assert.Equal(0u, (await context.Admin.QueueDeclarePassiveAsync(context.Source, token)).MessageCount);
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task SubscribeAsync_WithMissingOrFullTerminalDestination_RetainsUntilRepairedAsync(bool full, bool quorum)
    {
        Assert.SkipWhen(!fixture.IsAvailable, "RabbitMQ infrastructure not available");

        // Arrange
        using var timeout = CreateTimeout();
        var token = timeout.Token;
        await using var context = await RabbitMqReliabilityTestContext.CreateAsync(fixture.MessagingConnectionString!, Log, token);
        if (full)
        {
            await context.CreateDestinationAsync(token, bounded: true);
            await context.Admin.BasicPublishAsync(context.Dlx, "quarantine", true,
                new BasicProperties { MessageId = "test-owned-filler", Persistent = true }, Encoding.UTF8.GetBytes("filler"), token);
        }
        var options = context.Options(quorum);
        options.DeliveryLimit = 0;
        options.DeadLetterExchange = context.Dlx;
        options.DeadLetterRoutingKey = "quarantine";
        int handlers = 0;
        await using var bus = new RabbitMQMessageBus(options);
        await bus.SubscribeAsync<SimpleMessageA>(_ =>
        {
            Interlocked.Increment(ref handlers);
            throw new InvalidOperationException("Synthetic terminal failure");
        }, token);
        string id = Guid.NewGuid().ToString("N");
        // Act
        await bus.PublishAsync(new SimpleMessageA { Data = id }, new MessageOptions { UniqueId = id }, token);
        await RabbitMqReliabilityTestContext.WaitAsync(() => bus.LastDeliveryError is not null, token);
        // Assert
        Assert.True(GetSubscriberConnection(bus).IsOpen);
        Assert.False(bus.IsSubscriptionReady);
        if (full)
        {
            var filler = await context.Admin.BasicGetAsync(context.Destination, true, token);
            Assert.NotNull(filler);
            Assert.Equal("test-owned-filler", filler.BasicProperties.MessageId);
        }
        else
        {
            await context.CreateDestinationAsync(token);
        }
        var transferred = await context.ReadAsync(context.Destination, token);
        Assert.Equal(id, transferred.BasicProperties.MessageId);
        await RabbitMqReliabilityTestContext.WaitAsync(() => bus.ActiveDeliveryCount == 0, token);
        Assert.Equal(1, Volatile.Read(ref handlers));
        Assert.Equal(0u, (await context.Admin.QueueDeclarePassiveAsync(context.Source, token)).MessageCount);
        Assert.True(bus.IsSubscriptionReady);
        _logger.LogInformation("Terminal route repaired: full={DestinationWasFull}; handlerAttempts=1; confirmedId={MessageId}",
            full, id);
    }

    private static IConnection GetSubscriberConnection(RabbitMQMessageBus bus)
    {
        var field = typeof(RabbitMQMessageBus).GetField("_subscriberConnection", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(field);
        return Assert.IsAssignableFrom<IConnection>(field.GetValue(bus));
    }

    private CancellationTokenSource CreateTimeout()
    {
        var timeout = CancellationTokenSource.CreateLinkedTokenSource(TestCancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(40));
        return timeout;
    }

    private sealed class FaultedHandoffBus(RabbitMQMessageBusOptions options, bool publicationWasAccepted) : RabbitMQMessageBus(options)
    {
        private int _allowRecovery;
        private int _handoffAttempts;
        internal TaskCompletionSource FirstFailedAttempt { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal void AllowRecovery() => Volatile.Write(ref _allowRecovery, 1);

        protected override async Task PublishHandoffAsync(string exchange, string routingKey, BasicProperties properties,
            ReadOnlyMemory<byte> body, CancellationToken cancellationToken)
        {
            int attempt = Interlocked.Increment(ref _handoffAttempts);
            if (Volatile.Read(ref _allowRecovery) == 0)
            {
                if (publicationWasAccepted && attempt == 1)
                    await base.PublishHandoffAsync(exchange, routingKey, properties, body, cancellationToken);
                FirstFailedAttempt.TrySetResult();
                // For accepted=true, the real broker has already confirmed the copy.
                // The caller observes failure instead, modeling an ambiguous handoff result.
                throw new MessageBusException("Synthetic caller-observed handoff failure");
            }
            await base.PublishHandoffAsync(exchange, routingKey, properties, body, cancellationToken);
        }
    }
}
