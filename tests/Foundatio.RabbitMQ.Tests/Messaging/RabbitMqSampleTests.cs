using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Foundatio.Xunit;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using Xunit;

namespace Foundatio.RabbitMQ.Tests.Messaging;

[Collection(nameof(RabbitMqTestCollection))]
public class RabbitMqSampleTests(AspireFixture fixture, ITestOutputHelper output) : TestWithLoggingBase(output)
{
    [Fact]
    public async Task SubscribeAsync_WithUnavailablePrimaryAndReplacementHost_ProvisionsQuarantineAsync()
    {
        Assert.SkipWhen(!fixture.IsAvailable, "RabbitMQ infrastructure not available");

        // Arrange
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(TestCancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(45));
        var token = timeout.Token;
        await using var context = await RabbitMqReliabilityTestContext.CreateAsync(fixture.MessagingConnectionString!, Log, token);
        var endpoint = new Uri(fixture.MessagingConnectionString!);
        var start = new ProcessStartInfo("dotnet")
        {
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            CreateNoWindow = true
        };
        start.ArgumentList.Add(GetSubscriberPath());
        foreach (string argument in new[] { "--durable", "--acknowledgment-strategy", "automatic", "--require-successful-dispatch",
            "--topic", context.Topic, "--group-id", context.Topic, "--dead-letter-exchange", context.Dlx, "--provision-quarantine" })
            start.ArgumentList.Add(argument);
        start.Environment["ConnectionStrings__messaging"] = new UriBuilder(endpoint) { Port = 1 }.Uri.AbsoluteUri;
        start.Environment["RABBITMQ_HOSTS"] = $"{endpoint.Host}:{endpoint.Port}";
        string source = $"{context.Topic}-orderevent";
        string destination = $"{context.Dlx}-queue";

        // Act
        using var process = Process.Start(start) ?? throw new InvalidOperationException("Could not start the subscriber sample.");
        var errors = process.StandardError.ReadToEndAsync(token);
        try
        {
            bool ready = false;
            while (await process.StandardOutput.ReadLineAsync(token) is { } line)
            {
                if (!line.Contains("Waiting for messages.", StringComparison.Ordinal))
                    continue;
                ready = true;
                break;
            }

            // Assert
            Assert.True(ready, "Sample exited before provisioning/subscribing through the replacement endpoint.");
            Assert.Equal(1u, (await context.Admin.QueueDeclarePassiveAsync(source, token)).ConsumerCount);
            Assert.Equal(0u, (await context.Admin.QueueDeclarePassiveAsync(destination, token)).MessageCount);
        }
        finally
        {
            using var cleanup = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            if (!process.HasExited)
                process.Kill(entireProcessTree: true);
            await process.WaitForExitAsync(cleanup.Token);
            try
            {
                await errors.WaitAsync(cleanup.Token);
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested) { }
            var factory = new ConnectionFactory { Uri = endpoint };
            await using var connection = await factory.CreateConnectionAsync(cleanup.Token);
            foreach (string queue in new[] { source, destination })
            {
                await using var channel = await connection.CreateChannelAsync(cancellationToken: cleanup.Token);
                try
                {
                    await channel.QueueDeleteAsync(queue, cancellationToken: cleanup.Token);
                }
                catch (OperationInterruptedException exception) when (exception.ShutdownReason?.ReplyCode == 404) { }
            }
        }
    }

    private static string GetSubscriberPath()
    {
        var output = new DirectoryInfo(AppContext.BaseDirectory);
        string configuration = output.Parent!.Name;
        DirectoryInfo? root = output;
        while (root is not null && !File.Exists(Path.Combine(root.FullName, "Foundatio.RabbitMQ.slnx")))
            root = root.Parent;
        Assert.NotNull(root);
        string path = Path.Combine(root.FullName, "samples", "Foundatio.RabbitMQ.Subscribe", "bin", configuration,
            "net10.0", "Foundatio.RabbitMQ.Subscribe.dll");
        Assert.True(File.Exists(path), "Build the solution's samples before running this test.");
        return path;
    }
}
