using System;
using System.Threading;
using System.Threading.Tasks;
using Foundatio.Messaging;
using Foundatio.Xunit;
using RabbitMQ.Client;
using Xunit;

namespace Foundatio.RabbitMQ.Tests.Messaging;

[Collection(nameof(RabbitMqTestCollection))]
public class RabbitMqBrokerVersionTests(AspireFixture fixture, ITestOutputHelper output) : TestWithLoggingBase(output)
{
    [Theory]
    [InlineData("messaging")]
    [InlineData("messaging-delayed")]
    [InlineData("chaos-1")]
    [InlineData("chaos-2")]
    [InlineData("chaos-3")]
    [InlineData("messaging-tls")]
    public async Task CreateConnectionAsync_WithConfiguredBroker_UsesVersion425Async(string resource)
    {
        Assert.SkipWhen(!fixture.IsAvailable, "RabbitMQ infrastructure not available");

        // Arrange
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(TestCancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(30));
        string? connectionString = resource switch
        {
            "messaging" => fixture.MessagingConnectionString,
            "messaging-delayed" => fixture.MessagingDelayedConnectionString,
            "messaging-tls" => fixture.TlsConnectionString,
            _ => new ChaosTestHelper(fixture.App, Log).GetConnectionString(resource)
        };
        Assert.SkipWhen(String.IsNullOrWhiteSpace(connectionString), "Requested broker infrastructure not available");
        var factory = new ConnectionFactory { Uri = new Uri(connectionString!), AutomaticRecoveryEnabled = false };
        // Act
        await using var connection = await factory.CreateConnectionAsync(timeout.Token);
        // Assert
        Assert.Equal(new Version(4, 2, 5), RabbitMQMessageBus.ParseServerVersion(connection.ServerProperties));
    }
}
