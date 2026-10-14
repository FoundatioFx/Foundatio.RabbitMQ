using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Reflection;
using System.Security.Authentication;
using System.Threading;
using System.Threading.Tasks;
using Foundatio.Messaging;
using Foundatio.Xunit;
using RabbitMQ.Client;
using Xunit;

namespace Foundatio.RabbitMQ.Tests.Messaging;

[Collection(nameof(RabbitMqTestCollection))]
public class RabbitMqTlsTests(AspireFixture fixture, ITestOutputHelper output) : TestWithLoggingBase(output)
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CreateConnectionAsync_WithInvalidCertificate_RejectsExpectedAuthenticationFailureAsync(bool untrustedChain)
    {
        Assert.SkipWhen(String.IsNullOrEmpty(fixture.TlsConnectionString), "RabbitMQ TLS infrastructure not available");

        // Arrange
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(TestCancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(60));
        var validUri = GetBrokerUri(fixture.TlsConnectionString);
        await AssertHealthyControlAsync(validUri, timeout.Token);

        // Both addresses are loopback listeners owned by this Aspire fixture. The trusted
        // certificate covers localhost and 127.0.0.1, deliberately not 127.0.0.2.
        await using var wrongIdentityRelay = new RabbitMqTcpRelay(validUri, IPAddress.Parse("127.0.0.2"));
        var invalidUri = untrustedChain
            ? GetBrokerUri(fixture.UntrustedTlsConnectionString)
            : new UriBuilder(validUri) { Host = "127.0.0.2", Port = wrongIdentityRelay.Port }.Uri;
        var expectedErrors = untrustedChain
            ? SslPolicyErrors.RemoteCertificateChainErrors
            : SslPolicyErrors.RemoteCertificateNameMismatch;
        await AssertCertificateFailureAsync(invalidUri, expectedErrors, timeout.Token);

        await using var bus = CreateBus(invalidUri, replacementHostCount: 0);
        // Act
        var exception = await Record.ExceptionAsync(async () =>
        {
            await using var unexpected = await ConnectUsingProviderEndpointsAsync(bus, timeout.Token);
        });
        // Assert
        Assert.NotNull(exception);
        Assert.True(ContainsAuthenticationFailure(exception),
            "Expected a certificate authentication failure, not a connection refusal, timeout, or missing broker.");

        await AssertHealthyControlAsync(validUri, timeout.Token);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    public async Task CreateConnectionAsync_WithProviderEndpoints_ConnectsToTlsOnlyBrokerAsync(int replacementHostCount)
    {
        Assert.SkipWhen(String.IsNullOrEmpty(fixture.TlsConnectionString), "RabbitMQ TLS infrastructure not available");

        // Arrange
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(TestCancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(30));
        var uri = GetBrokerUri(fixture.TlsConnectionString);
        await using var bus = CreateBus(uri, replacementHostCount);
        // Act
        await using var connection = await ConnectUsingProviderEndpointsAsync(bus, timeout.Token);

        // Assert
        Assert.True(connection.IsOpen);
        Assert.Equal(new Version(4, 2, 5), RabbitMQMessageBus.ParseServerVersion(connection.ServerProperties));
        Assert.True(connection.Endpoint.Ssl.Enabled);
        Assert.Equal(uri.Port, connection.Endpoint.Port);
        Assert.Equal(SslPolicyErrors.None, connection.Endpoint.Ssl.AcceptablePolicyErrors);
    }

    [Fact]
    public async Task PublishAsync_WithTlsPublisherAndSubscriber_DeliversIdentifiedMessageAsync()
    {
        Assert.SkipWhen(String.IsNullOrEmpty(fixture.TlsConnectionString), "RabbitMQ TLS infrastructure not available");

        // Arrange
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(TestCancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(60));
        var uri = GetBrokerUri(fixture.TlsConnectionString);
        await using var bus = CreateBus(uri, replacementHostCount: 2);
        string expectedId = Guid.NewGuid().ToString("N");
        var received = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);

        // Act
        await bus.SubscribeAsync<TlsProbe>((message, _) =>
        {
            received.TrySetResult(message.Id);
            return Task.CompletedTask;
        }, cancellationToken: timeout.Token).WaitAsync(timeout.Token);
        await bus.PublishAsync(new TlsProbe { Id = expectedId }, cancellationToken: timeout.Token).WaitAsync(timeout.Token);

        // Assert
        Assert.Equal(expectedId, await received.Task.WaitAsync(timeout.Token));
        foreach (string name in new[] { "_publisherConnection", "_subscriberConnection" })
        {
            var connection = GetField<IConnection>(bus, name);
            Assert.True(connection.IsOpen);
            Assert.Equal(new Version(4, 2, 5), RabbitMQMessageBus.ParseServerVersion(connection.ServerProperties));
            Assert.True(connection.Endpoint.Ssl.Enabled);
            Assert.Equal(SslPolicyErrors.None, connection.Endpoint.Ssl.AcceptablePolicyErrors);
        }
        // The fixture disables plaintext AMQP listeners. Successfully exchanging an
        // identified AMQP message therefore also requires real TLS negotiation.
    }

    private RabbitMQMessageBus CreateBus(Uri uri, int replacementHostCount)
    {
        string[]? hosts = replacementHostCount switch
        {
            0 => null,
            1 => [$"localhost:{uri.Port}"],
            2 => [$"localhost:{uri.Port}", $"127.0.0.1:{uri.Port}"],
            _ => throw new ArgumentOutOfRangeException(nameof(replacementHostCount))
        };
        // An unusable URI host proves that Hosts replaces, rather than supplements,
        // the connection URI endpoint. Credentials and vhost still come from the URI.
        var connectionUri = hosts is null ? uri : new UriBuilder(uri) { Host = "unused.invalid" }.Uri;
        return new RabbitMQMessageBus(new RabbitMQMessageBusOptions
        {
            ConnectionString = connectionUri.AbsoluteUri,
            LoggerFactory = Log,
            Hosts = hosts,
            Topic = $"tls-verification-{Guid.NewGuid():N}",
            SubscriptionQueueName = $"tls-verification-{Guid.NewGuid():N}",
            IsDurable = false,
            IsSubscriptionQueueExclusive = true,
            SubscriptionQueueAutoDelete = true,
            AcknowledgementStrategy = AcknowledgementStrategy.Automatic,
            PublisherConfirmsEnabled = true,
            PrefetchCount = 1
        });
    }

    private static Task<IConnection> ConnectUsingProviderEndpointsAsync(RabbitMQMessageBus bus, CancellationToken cancellationToken)
    {
        var factory = GetField<ConnectionFactory>(bus, "_factory");
        factory.RequestedConnectionTimeout = TimeSpan.FromSeconds(10);
        return factory.CreateConnectionAsync(
            GetField<List<AmqpTcpEndpoint>>(bus, "_endpoints"), cancellationToken: cancellationToken);
    }

    private static async Task AssertHealthyControlAsync(Uri uri, CancellationToken cancellationToken)
    {
        var factory = new ConnectionFactory
        {
            Uri = uri,
            AutomaticRecoveryEnabled = false,
            RequestedConnectionTimeout = TimeSpan.FromSeconds(10)
        };
        factory.Ssl.AcceptablePolicyErrors = SslPolicyErrors.None;
        await using var connection = await factory.CreateConnectionAsync(cancellationToken);
        Assert.True(connection.IsOpen);
        Assert.Equal(new Version(4, 2, 5), RabbitMQMessageBus.ParseServerVersion(connection.ServerProperties));
    }

    private static async Task AssertCertificateFailureAsync(Uri uri, SslPolicyErrors expectedErrors, CancellationToken cancellationToken)
    {
        using var client = new TcpClient();
        await client.ConnectAsync(uri.DnsSafeHost, uri.Port, cancellationToken);
        SslPolicyErrors? observedErrors = null;
        await using var ssl = new SslStream(client.GetStream(), leaveInnerStreamOpen: false, (_, _, _, errors) =>
        {
            observedErrors = errors;
            // Observe the failure without ever accepting an invalid certificate.
            return errors == SslPolicyErrors.None;
        });
        await Assert.ThrowsAnyAsync<AuthenticationException>(() => ssl.AuthenticateAsClientAsync(
            new SslClientAuthenticationOptions { TargetHost = uri.DnsSafeHost }, cancellationToken));
        Assert.Equal(expectedErrors, observedErrors);
    }

    private static bool ContainsAuthenticationFailure(Exception exception) =>
        exception is AuthenticationException ||
        (exception is AggregateException aggregate && aggregate.InnerExceptions.Any(ContainsAuthenticationFailure)) ||
        (exception.InnerException is not null && ContainsAuthenticationFailure(exception.InnerException));

    private Uri GetBrokerUri(string? value)
    {
        Assert.True(Uri.TryCreate(value, UriKind.Absolute, out var uri), "The Aspire fixture must provide its TLS broker URI.");
        Assert.Equal("amqps", uri!.Scheme);
        Assert.True(uri.IsLoopback, "TLS verification only connects to test-owned loopback brokers.");
        Assert.InRange(uri.Port, 1, 65535);
        return uri;
    }

    private static T GetField<T>(RabbitMQMessageBus bus, string name)
    {
        var field = typeof(RabbitMQMessageBus).GetField(name, BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(field);
        return Assert.IsAssignableFrom<T>(field.GetValue(bus));
    }

    public sealed class TlsProbe
    {
        public string Id { get; set; } = String.Empty;
    }
}
