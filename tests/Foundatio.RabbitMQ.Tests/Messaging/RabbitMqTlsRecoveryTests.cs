using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Net;
using System.Net.Security;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Foundatio.Messaging;
using Foundatio.Xunit;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using Xunit;

namespace Foundatio.RabbitMQ.Tests.Messaging;

// The relays forward opaque bytes. TLS still terminates at the TLS-only broker;
// no certificate-validation callback or plaintext AMQP listener is introduced.
[Collection(nameof(RabbitMqTestCollection))]
public class RabbitMqTlsRecoveryTests(AspireFixture fixture, ITestOutputHelper output) : TestWithLoggingBase(output)
{
    [Theory]
    [InlineData("_publisherConnection")]
    [InlineData("_subscriberConnection")]
    public async Task PublishAsync_AfterEstablishedTlsPathLoss_RecoversAndDrainsPendingWorkAsync(string targetRole)
    {
        Assert.SkipWhen(String.IsNullOrEmpty(fixture.TlsConnectionString), "RabbitMQ TLS infrastructure not available");

        // Arrange
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(TestCancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(90));
        var broker = GetBrokerUri();
        string topic = $"tls-recovery-{Guid.NewGuid():N}";
        var adminFactory = new ConnectionFactory { Uri = broker, AutomaticRecoveryEnabled = false };
        adminFactory.Ssl.AcceptablePolicyErrors = SslPolicyErrors.None;
        await using var adminConnection = await adminFactory.CreateConnectionAsync(timeout.Token);
        await using var admin = await adminConnection.CreateChannelAsync(cancellationToken: timeout.Token);
        var controlOptions = CreateOptions(broker, topic);
        controlOptions.IsDurable = true;
        await using var control = new RabbitMQMessageBus(controlOptions);
        // Own the initial listeners even if later setup fails before the fault-test
        // cleanup block is entered. Restarted listeners are disposed in that block.
        await using var firstLease = new RabbitMqTcpRelay(broker, IPAddress.Loopback);
        await using var secondLease = new RabbitMqTcpRelay(broker, IPAddress.Loopback);
        var first = firstLease;
        var second = secondLease;
        int firstPort = first.Port;
        int secondPort = second.Port;
        var options = CreateOptions(broker, topic);
        options.Hosts = [$"127.0.0.1:{firstPort}", $"127.0.0.1:{secondPort}"];
        options.IsDurable = true;
        options.IsSubscriptionQueueExclusive = false;
        options.SubscriptionQueueAutoDelete = false;
        options.RequestedHeartbeat = TimeSpan.FromSeconds(2);
        options.NetworkRecoveryInterval = TimeSpan.FromSeconds(1);
        var receipts = new ConcurrentDictionary<string, TaskCompletionSource<string>>();
        var deliveries = new ConcurrentDictionary<string, int>();
        var bus = new RabbitMQMessageBus(options);

        try
        {
            await bus.SubscribeAsync<Probe>((message, _) =>
            {
                deliveries.AddOrUpdate(message.Id, 1, (_, count) => count + 1);
                if (receipts.TryGetValue(message.Id, out var completion))
                    completion.TrySetResult(message.Id);
                return Task.CompletedTask;
            }, cancellationToken: timeout.Token).WaitAsync(timeout.Token);

            string warmup = AddExpected(receipts);
            await bus.PublishAsync(new Probe { Id = warmup }, cancellationToken: timeout.Token).WaitAsync(timeout.Token);
            Assert.Equal(warmup, await receipts[warmup].Task.WaitAsync(timeout.Token));
            var publisher = GetConnection(bus, "_publisherConnection");
            var subscriber = GetConnection(bus, "_subscriberConnection");
            var target = GetConnection(bus, targetRole);
            AssertTls(publisher);
            AssertTls(subscriber);
            int interruptedPort = target.Endpoint.Port;
            Assert.Contains(interruptedPort, new[] { firstPort, secondPort });
            int alternatePort = interruptedPort == firstPort ? secondPort : firstPort;
            var publisherShutdown = ObserveShutdownAsync(publisher);
            var subscriberShutdown = ObserveShutdownAsync(subscriber);
            var publisherRecovery = ObserveRecoveryAsync(publisher);
            var subscriberRecovery = ObserveRecoveryAsync(subscriber);
            _logger.LogInformation("Established TLS sessions: publisher={PublisherPort}, subscriber={SubscriberPort}; target={TargetRole}, interrupted={InterruptedPort}, alternate={AlternatePort}",
                publisher.Endpoint.Port, subscriber.Endpoint.Port, targetRole, interruptedPort, alternatePort);

            // Close both test-owned paths, including the observed active endpoint.
            // Keeping both unavailable temporarily makes the pending-work assertion
            // deterministic. The backend and its shared queue remain available.
            // Act
            await first.DisposeAsync();
            await second.DisposeAsync();
            await Task.WhenAll(publisherShutdown, subscriberShutdown).WaitAsync(timeout.Token);
            // Assert
            Assert.False(publisher.IsOpen);
            Assert.False(subscriber.IsOpen);
            string pending = AddExpected(receipts);
            await control.PublishAsync(new Probe { Id = pending }, cancellationToken: timeout.Token).WaitAsync(timeout.Token);
            var queue = await admin.QueueDeclarePassiveAsync(options.SubscriptionQueueName, timeout.Token);
            Assert.True(queue.MessageCount > 0, "The shared broker must retain pending work while both client paths are down.");
            Assert.False(receipts[pending].Task.IsCompleted);
            _logger.LogInformation("Both client paths down: retainedMessages={RetainedMessages}, pendingId={MessageId}, delivered=false",
                queue.MessageCount, pending);

            // Restore only the other endpoint. Recovery cannot accidentally pass by
            // reconnecting the target connection to its original endpoint.
            if (alternatePort == firstPort)
                first = new RabbitMqTcpRelay(broker, IPAddress.Loopback, firstPort);
            else
                second = new RabbitMqTcpRelay(broker, IPAddress.Loopback, secondPort);
            await Task.WhenAll(publisherRecovery, subscriberRecovery).WaitAsync(timeout.Token);
            Assert.Equal(alternatePort, target.Endpoint.Port);
            Assert.Same(publisher, GetConnection(bus, "_publisherConnection"));
            Assert.Same(subscriber, GetConnection(bus, "_subscriberConnection"));
            AssertTls(publisher);
            AssertTls(subscriber);
            Assert.Equal(pending, await receipts[pending].Task.WaitAsync(timeout.Token));

            string afterRecovery = AddExpected(receipts);
            await bus.PublishAsync(new Probe { Id = afterRecovery }, cancellationToken: timeout.Token).WaitAsync(timeout.Token);
            Assert.Equal(afterRecovery, await receipts[afterRecovery].Task.WaitAsync(timeout.Token));
            Assert.Equal(receipts.Keys.OrderBy(id => id), deliveries.Keys.OrderBy(id => id));
            _logger.LogInformation("Recovered TLS sessions: publisher={PublisherPort}, subscriber={SubscriberPort}; expected={ExpectedIds}; duplicate deliveries={DuplicateDeliveries}",
                publisher.Endpoint.Port, subscriber.Endpoint.Port, String.Join(',', receipts.Keys), deliveries.Values.Sum(count => count - 1));
        }
        finally
        {
            // Restore the interrupted listener before cleanup, including assertion
            // failures. Never alter the backend broker or leave a fault active.
            try
            {
                if (first.IsStopped)
                    first = new RabbitMqTcpRelay(broker, IPAddress.Loopback, firstPort);
                if (second.IsStopped)
                    second = new RabbitMqTcpRelay(broker, IPAddress.Loopback, secondPort);
                await bus.DisposeAsync();
                using var cleanup = new CancellationTokenSource(TimeSpan.FromSeconds(10));
                await admin.QueueDeleteAsync(options.SubscriptionQueueName, cancellationToken: cleanup.Token);
                await admin.ExchangeDeleteAsync(topic, cancellationToken: cleanup.Token);
            }
            finally
            {
                await first.DisposeAsync();
                await second.DisposeAsync();
            }
        }
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(true, true)]
    public async Task PublishAsync_WithCustomTlsPort_DeliversThroughConfiguredEndpointAsync(bool replacementHost, bool ipv6)
    {
        Assert.SkipWhen(String.IsNullOrEmpty(fixture.TlsConnectionString), "RabbitMQ TLS infrastructure not available");

        // Arrange
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(TestCancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(60));
        var broker = GetBrokerUri();
        var address = ipv6 ? IPAddress.IPv6Loopback : IPAddress.Loopback;
        await using var relay = new RabbitMqTcpRelay(broker, address);
        var endpoint = new UriBuilder(broker) { Host = address.ToString(), Port = relay.Port }.Uri;
        string host = ipv6 ? $"[::1]:{relay.Port}" : $"127.0.0.1:{relay.Port}";
        var options = CreateOptions(endpoint, $"tls-port-{Guid.NewGuid():N}");
        if (replacementHost)
        {
            options.ConnectionString = new UriBuilder(broker) { Host = "unused.invalid" }.Uri.AbsoluteUri;
            options.Hosts = [host];
        }
        await using var bus = new RabbitMQMessageBus(options);
        var received = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        string id = Guid.NewGuid().ToString("N");
        // Act
        await bus.SubscribeAsync<Probe>((message, _) =>
        {
            received.TrySetResult(message.Id);
            return Task.CompletedTask;
        }, cancellationToken: timeout.Token).WaitAsync(timeout.Token);
        await bus.PublishAsync(new Probe { Id = id }, cancellationToken: timeout.Token).WaitAsync(timeout.Token);
        // Assert
        Assert.Equal(id, await received.Task.WaitAsync(timeout.Token));
        foreach (string role in new[] { "_publisherConnection", "_subscriberConnection" })
        {
            var connection = GetConnection(bus, role);
            AssertTls(connection);
            Assert.Equal(relay.Port, connection.Endpoint.Port);
            Assert.Equal(address.ToString(), connection.Endpoint.Ssl.ServerName);
        }
        _logger.LogInformation("Verified TLS delivery: replacementHost={ReplacementHost}, address={Address}, port={Port}, id={MessageId}",
            replacementHost, address, relay.Port, id);
    }

    private static string AddExpected(ConcurrentDictionary<string, TaskCompletionSource<string>> receipts)
    {
        string id = Guid.NewGuid().ToString("N");
        Assert.True(receipts.TryAdd(id, new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously)));
        return id;
    }

    private static Task ObserveShutdownAsync(IConnection connection)
    {
        var completion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        connection.ConnectionShutdownAsync += (_, _) =>
        {
            completion.TrySetResult();
            return Task.CompletedTask;
        };
        return completion.Task;
    }

    private static Task ObserveRecoveryAsync(IConnection connection)
    {
        var completion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        connection.RecoverySucceededAsync += (_, _) =>
        {
            completion.TrySetResult();
            return Task.CompletedTask;
        };
        return completion.Task;
    }

    private RabbitMQMessageBusOptions CreateOptions(Uri uri, string topic) => new()
    {
        ConnectionString = uri.AbsoluteUri,
        LoggerFactory = Log,
        Topic = topic,
        SubscriptionQueueName = $"{topic}-subscription",
        IsDurable = false,
        IsSubscriptionQueueExclusive = true,
        SubscriptionQueueAutoDelete = true,
        AcknowledgementStrategy = AcknowledgementStrategy.Automatic,
        PublisherConfirmsEnabled = true,
        PrefetchCount = 1
    };

    private Uri GetBrokerUri()
    {
        string? value = fixture.TlsConnectionString;
        Assert.True(Uri.TryCreate(value, UriKind.Absolute, out var uri), "The Aspire fixture must provide its TLS broker URI.");
        Assert.Equal("amqps", uri!.Scheme);
        Assert.True(uri.IsLoopback);
        return uri;
    }

    private static IConnection GetConnection(RabbitMQMessageBus bus, string name)
    {
        var field = typeof(RabbitMQMessageBus).GetField(name, BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(field);
        return Assert.IsAssignableFrom<IConnection>(field.GetValue(bus));
    }

    private static void AssertTls(IConnection connection)
    {
        Assert.True(connection.IsOpen);
        Assert.Equal(new Version(4, 2, 5), RabbitMQMessageBus.ParseServerVersion(connection.ServerProperties));
        Assert.True(connection.Endpoint.Ssl.Enabled);
        Assert.Equal(SslPolicyErrors.None, connection.Endpoint.Ssl.AcceptablePolicyErrors);
    }

    public sealed class Probe
    {
        public string Id { get; set; } = String.Empty;
    }
}
