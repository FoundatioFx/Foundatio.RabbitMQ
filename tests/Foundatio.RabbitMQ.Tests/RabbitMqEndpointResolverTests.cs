using System;
using System.Net.Security;
using Foundatio.Utility;
using Foundatio.Xunit;
using RabbitMQ.Client;
using Xunit;

namespace Foundatio.RabbitMQ.Tests;

public class RabbitMqEndpointResolverTests(ITestOutputHelper output) : TestWithLoggingBase(output)
{
    [Theory]
    [InlineData("broker:0")]
    [InlineData("broker:65536")]
    [InlineData("broker:")]
    [InlineData("broker:invalid")]
    [InlineData("[::1")]
    [InlineData("[::1]unexpected")]
    [InlineData(" ")]
    public void CreateEndpoints_WithInvalidHost_RejectsConfiguration(string host)
    {
        // Arrange
        var factory = new ConnectionFactory { Uri = new Uri("amqps://localhost") };

        // Act
        var exception = Record.Exception(() => RabbitMQEndpointResolver.CreateEndpoints(factory, [host]));

        // Assert
        Assert.IsAssignableFrom<ArgumentException>(exception);
    }

    [Fact]
    public void CreateEndpoints_WithNullFactory_ThrowsArgumentNullException()
    {
        // Arrange
        ConnectionFactory factory = null!;

        // Act
        var exception = Record.Exception(() => RabbitMQEndpointResolver.CreateEndpoints(factory));

        // Assert
        Assert.IsType<ArgumentNullException>(exception);
    }

    [Theory]
    [InlineData("amqp://localhost", 5672, false)]
    [InlineData("amqps://localhost", 5671, true)]
    [InlineData("amqps://localhost:5680", 5680, true)]
    public void CreateEndpoints_WithoutReplacementHosts_UsesFactoryEndpoint(string uri, int port, bool useTls)
    {
        // Arrange
        var factory = new ConnectionFactory { Uri = new Uri(uri) };

        // Act
        var endpoints = RabbitMQEndpointResolver.CreateEndpoints(factory);

        // Assert
        var endpoint = Assert.Single(endpoints);
        Assert.Equal("localhost", endpoint.HostName);
        Assert.Equal(port, endpoint.Port);
        Assert.Equal(useTls, endpoint.Ssl.Enabled);
        Assert.Equal("localhost", endpoint.Ssl.ServerName);
        Assert.Equal(SslPolicyErrors.None, endpoint.Ssl.AcceptablePolicyErrors);
    }

    [Theory]
    [InlineData("broker", "broker", 5671)]
    [InlineData("broker:5680", "broker", 5680)]
    [InlineData("[::1]:5680", "::1", 5680)]
    [InlineData("::1", "::1", 5671)]
    public void CreateEndpoints_WithReplacementHost_PreservesTlsIdentity(string host, string hostname, int port)
    {
        // Arrange
        var factory = new ConnectionFactory { Uri = new Uri("amqps://localhost") };

        // Act
        var endpoints = RabbitMQEndpointResolver.CreateEndpoints(factory, [host, host, " "]);

        // Assert
        var endpoint = Assert.Single(endpoints);
        Assert.Equal(hostname, endpoint.HostName);
        Assert.Equal(port, endpoint.Port);
        Assert.True(endpoint.Ssl.Enabled);
        Assert.Equal(hostname, endpoint.Ssl.ServerName);
        Assert.Equal(SslPolicyErrors.None, endpoint.Ssl.AcceptablePolicyErrors);
    }
}
