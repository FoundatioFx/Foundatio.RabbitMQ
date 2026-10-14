using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Foundatio.Xunit;
using Xunit;

namespace Foundatio.RabbitMQ.Tests;

public class RabbitMqTcpRelayTests(ITestOutputHelper output) : TestWithLoggingBase(output)
{
    [Fact]
    public async Task DisposeAsync_WithActiveTunnel_ClosesConnectionsAndSupportsRepeatedDisposal()
    {
        // Arrange
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(TestCancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(10));
        using var backend = new TcpListener(IPAddress.Loopback, 0);
        backend.Start();
        int backendPort = ((IPEndPoint)backend.LocalEndpoint).Port;
        await using var relay = new RabbitMqTcpRelay(new Uri($"tcp://localhost:{backendPort}"), IPAddress.Loopback);
        using var client = new TcpClient();
        await client.ConnectAsync(IPAddress.Loopback, relay.Port, timeout.Token);
        using var upstream = await backend.AcceptTcpClientAsync(timeout.Token);
        byte[] sent = [1, 2, 3];
        byte[] received = new byte[sent.Length];

        // Act
        await client.GetStream().WriteAsync(sent, timeout.Token);
        await upstream.GetStream().ReadExactlyAsync(received, timeout.Token);
        await upstream.GetStream().WriteAsync(received, timeout.Token);
        byte[] echoed = new byte[sent.Length];
        await client.GetStream().ReadExactlyAsync(echoed, timeout.Token);
        await Task.WhenAll(relay.DisposeAsync().AsTask(), relay.DisposeAsync().AsTask()).WaitAsync(timeout.Token);

        // Assert
        Assert.Equal(sent, received);
        Assert.Equal(sent, echoed);
        Assert.True(relay.IsStopped);
        Assert.Equal(0, await client.GetStream().ReadAsync(new byte[1], timeout.Token));
        Assert.Equal(0, await upstream.GetStream().ReadAsync(new byte[1], timeout.Token));
    }
}
