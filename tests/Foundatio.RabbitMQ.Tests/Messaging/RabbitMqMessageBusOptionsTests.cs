using System;
using System.Collections.Generic;
using System.Globalization;
using System.Net.Security;
using System.Reflection;
using System.Threading.Tasks;
using Foundatio.AsyncEx;
using Foundatio.Messaging;
using Foundatio.Xunit;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Xunit;

namespace Foundatio.RabbitMQ.Tests.Messaging;

public class RabbitMqMessageBusOptionsTests(ITestOutputHelper output) : TestWithLoggingBase(output)
{
    [Theory]
    [InlineData("amqp://localhost", false, 5672)]
    [InlineData("amqp://localhost:5678", false, 5678)]
    [InlineData("amqps://localhost", true, 5671)]
    [InlineData("amqps://localhost:5679", true, 5679)]
    public async Task Constructor_WithConnectionUri_PreservesEndpointTransportAsync(string connectionString, bool encrypted, int port)
    {
        // Act
        await using var bus = new RabbitMQMessageBus(o => o.LoggerFactory(Log).ConnectionString(connectionString));

        // Assert
        var endpoint = Assert.Single(GetEndpoints(bus));
        Assert.Equal("localhost", endpoint.HostName);
        Assert.Equal(port, endpoint.Port);
        Assert.Equal(encrypted, endpoint.Ssl.Enabled);
        Assert.Equal(SslPolicyErrors.None, endpoint.Ssl.AcceptablePolicyErrors);
        if (encrypted)
            Assert.Equal("localhost", endpoint.Ssl.ServerName);
    }

    [Theory]
    [InlineData("broker:0")]
    [InlineData("broker:-1")]
    [InlineData("broker:65536")]
    [InlineData("broker:not-a-port")]
    [InlineData("broker:")]
    [InlineData(":5672")]
    [InlineData("[::1")]
    [InlineData("[::1]unexpected")]
    [InlineData("[::1]:0")]
    [InlineData("[]:5672")]
    [InlineData(" ")]
    public void Constructor_WithInvalidHost_RejectsConfigurationWithoutEchoingCredentials(string host)
    {
        // Arrange
        var options = new RabbitMQMessageBusOptions
        {
            LoggerFactory = Log,
            ConnectionString = "amqps://test-user:test-password@localhost",
            Hosts = [host]
        };

        // Act
        var exception = Record.Exception(() => new RabbitMQMessageBus(options));

        // Assert
        var argumentException = Assert.IsAssignableFrom<ArgumentException>(exception);

        Assert.DoesNotContain("test-password", argumentException.Message);
        Assert.DoesNotContain("test-user", argumentException.Message);
    }

    [Theory]
    [InlineData("[::1]", "::1", 5671)]
    [InlineData("[::1]:5680", "::1", 5680)]
    [InlineData("::1", "::1", 5671)]
    public async Task Constructor_WithIpv6Host_PreservesAddressAndTlsAsync(string host, string expectedHost, int expectedPort)
    {
        // Act
        await using var bus = new RabbitMQMessageBus(o => o.LoggerFactory(Log).ConnectionString("amqps://localhost").Hosts(host));

        // Assert
        var endpoint = Assert.Single(GetEndpoints(bus));
        Assert.Equal(expectedHost, endpoint.HostName);
        Assert.Equal(expectedPort, endpoint.Port);
        Assert.True(endpoint.Ssl.Enabled);
        Assert.Equal(expectedHost, endpoint.Ssl.ServerName);
    }

    [Fact]
    public async Task Constructor_WithIpv6Uri_UsesUnbracketedEndpointIdentityAsync()
    {
        // Act
        await using var bus = new RabbitMQMessageBus(o => o.LoggerFactory(Log).ConnectionString("amqps://[::1]:5680"));

        // Assert
        var endpoint = Assert.Single(GetEndpoints(bus));
        Assert.Equal("::1", endpoint.HostName);
        Assert.Equal("::1", endpoint.Ssl.ServerName);
        Assert.True(endpoint.Ssl.Enabled);
        Assert.Equal(5680, endpoint.Port);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Constructor_WithReplacementHosts_PreservesTlsAndCredentialsAsync(bool useBuilder)
    {
        // Arrange
        const string connectionString = "amqps://test-user:test-password@original.example:5679/test-vhost";
        string[] hosts = ["first.example", "second.example:5680", "FIRST.example", " "];
        // Act
        await using var bus = useBuilder
            ? new RabbitMQMessageBus(o => o.LoggerFactory(Log).ConnectionString(connectionString).Hosts(hosts))
            : new RabbitMQMessageBus(new RabbitMQMessageBusOptions { ConnectionString = connectionString, LoggerFactory = Log, Hosts = hosts });

        // Assert
        var endpoints = GetEndpoints(bus);
        Assert.Equal(2, endpoints.Count);
        Assert.Equal("first.example", endpoints[0].HostName);
        Assert.Equal(5671, endpoints[0].Port);
        Assert.Equal("second.example", endpoints[1].HostName);
        Assert.Equal(5680, endpoints[1].Port);
        Assert.NotSame(endpoints[0].Ssl, endpoints[1].Ssl);
        Assert.All(endpoints, endpoint =>
        {
            Assert.True(endpoint.Ssl.Enabled);
            Assert.Equal(endpoint.HostName, endpoint.Ssl.ServerName);
            Assert.Equal(SslPolicyErrors.None, endpoint.Ssl.AcceptablePolicyErrors);
        });

        var factory = GetField<ConnectionFactory>(bus, "_factory");
        Assert.Equal("test-user", factory.UserName);
        Assert.Equal("test-password", factory.Password);
        Assert.Equal("test-vhost", factory.VirtualHost);
        Assert.All(endpoints, endpoint => Assert.Equal(factory.Ssl.Version, endpoint.Ssl.Version));
    }

    [Fact]
    public void Constructor_WithRequiredDispatchAndFireAndForget_ThrowsArgumentException()
    {
        // Arrange
        var options = new RabbitMQMessageBusOptions
        {
            ConnectionString = "amqp://localhost",
            LoggerFactory = Log,
            RequireSuccessfulDispatch = true
        };

        // Act
        var exception = Record.Exception(() => new RabbitMQMessageBus(options));

        // Assert
        Assert.IsType<ArgumentException>(exception);
    }

    [Fact]
    public void Constructor_WithRequiredDispatchAndNoDestination_ThrowsArgumentException()
    {
        // Arrange
        var options = new RabbitMQMessageBusOptions
        {
            ConnectionString = "amqp://localhost",
            LoggerFactory = Log,
            AcknowledgementStrategy = AcknowledgementStrategy.Automatic,
            RequireSuccessfulDispatch = true
        };

        // Act
        var exception = Assert.Throws<ArgumentException>(() => new RabbitMQMessageBus(options));

        // Assert
        Assert.Contains(nameof(options.DeadLetterExchange), exception.Message);
    }

    [Fact]
    public void Constructor_WithZeroUriPort_RejectsConfiguration()
    {
        // Arrange
        var options = new RabbitMQMessageBusOptions { LoggerFactory = Log, ConnectionString = "amqps://localhost:0" };

        // Act
        var exception = Record.Exception(() => new RabbitMQMessageBus(options));

        // Assert
        Assert.IsAssignableFrom<ArgumentException>(exception);
    }

    [Fact]
    public async Task ConvertToMessage_WithNumericHeaders_UsesInvariantCultureAsync()
    {
        // Arrange
        var originalCulture = CultureInfo.CurrentCulture;
        await using var bus = new LifecycleOverrideBus(new RabbitMQMessageBusOptions
        {
            ConnectionString = "amqp://localhost",
            LoggerFactory = Log
        });
        var properties = new BasicProperties
        {
            Headers = new Dictionary<string, object?> { ["amount"] = 1234.5m }
        };
        var envelope = new BasicDeliverEventArgs("consumer", 1, false, "source", "route", properties, new byte[] { 1 });
        try
        {
            CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo("fr-FR");

            // Act
            var message = bus.Convert(envelope);

            // Assert
            Assert.Equal("1234.5", message.Properties["amount"]);
        }
        finally
        {
            CultureInfo.CurrentCulture = originalCulture;
        }
    }

    [Fact]
    public void CopyHandoffProperties_WithPublisherIdentityAndRoutingHeaders_RemovesTransportControls()
    {
        // Arrange
        var original = new BasicProperties
        {
            MessageId = "stable-id",
            UserId = "original-publisher",
            Persistent = true,
            Headers = new Dictionary<string, object?>
            {
                ["CC"] = new object[] { "unintended-queue" },
                ["BCC"] = new object[] { "hidden-queue" },
                ["x-delay"] = 1000,
                ["application-header"] = "preserved"
            }
        };
        var envelope = new BasicDeliverEventArgs("consumer", 1, false, "source", "route", original, new byte[] { 1 });

        // Act
        var properties = RabbitMQMessageConverter.CopyHandoffProperties(envelope);

        // Assert
        Assert.Null(properties.UserId);
        Assert.DoesNotContain("CC", properties.Headers!);
        Assert.DoesNotContain("BCC", properties.Headers!);
        Assert.DoesNotContain("x-delay", properties.Headers!);
        Assert.Equal("stable-id", properties.MessageId);
        Assert.Equal("preserved", properties.Headers!["application-header"]);
        Assert.True(properties.Persistent);
        Assert.Equal("original-publisher", original.UserId);
        Assert.Contains("CC", original.Headers);
    }

    [Theory]
    [InlineData("_subscriberLock")]
    [InlineData("_lock")]
    [InlineData("_handoffLock")]
    public async Task DisposeAsync_WithBusyTransportLock_ReturnsWithinTimeoutAsync(string field)
    {
        // Arrange
        var bus = new RabbitMQMessageBus(new RabbitMQMessageBusOptions
        {
            ConnectionString = "amqp://localhost",
            LoggerFactory = Log,
            ShutdownTimeout = TimeSpan.FromMilliseconds(50)
        });
        Task disposing;
        using (await GetField<AsyncLock>(bus, field).LockAsync(TestCancellationToken))
        {
            // Act
            disposing = bus.DisposeAsync().AsTask();

            // Assert
            await disposing.WaitAsync(TimeSpan.FromSeconds(2), TestCancellationToken);
        }
        await disposing;
    }

    [Fact]
    public async Task DisposeAsync_WithLifecycleOverride_InvokesRemovalHookAsync()
    {
        // Arrange
        var bus = new LifecycleOverrideBus(new RabbitMQMessageBusOptions
        {
            ConnectionString = "amqp://localhost",
            LoggerFactory = Log
        });

        // Act
        await bus.DisposeAsync();

        // Assert
        Assert.Equal(1, bus.Removals);
    }

    private static List<AmqpTcpEndpoint> GetEndpoints(RabbitMQMessageBus bus) => GetField<List<AmqpTcpEndpoint>>(bus, "_endpoints");

    private sealed class LifecycleOverrideBus(RabbitMQMessageBusOptions options) : RabbitMQMessageBus(options)
    {
        internal int Removals { get; private set; }
        internal IMessage Convert(BasicDeliverEventArgs envelope) => ConvertToMessage(envelope);

        protected override Task RemoveTopicSubscriptionAsync()
        {
            Removals++;
            return base.RemoveTopicSubscriptionAsync();
        }
    }

    private static T GetField<T>(RabbitMQMessageBus bus, string name)
    {
        var field = typeof(RabbitMQMessageBus).GetField(name, BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(field);
        return Assert.IsType<T>(field.GetValue(bus));
    }
}
