using System;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Foundatio.Messaging;
using Foundatio.Xunit;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Foundatio.RabbitMQ.Tests.Messaging;

[Collection(nameof(RabbitMqTestCollection))]
public class RabbitMqDelayedProcessTests(AspireFixture fixture, ITestOutputHelper output) : TestWithLoggingBase(output)
{
    [Fact]
    public async Task PublishAsync_WithBrokerDelayAndPublisherProcessTermination_DeliversScheduledMessageAsync()
    {
        Assert.SkipWhen(String.IsNullOrEmpty(fixture.MessagingDelayedConnectionString), "Delayed broker unavailable");

        // Arrange
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(TestCancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(90));
        var token = timeout.Token;
        await using var context = await RabbitMqReliabilityTestContext.CreateAsync(fixture.MessagingDelayedConnectionString!, Log, token);
        var options = context.Options();
        await using (var provision = new RabbitMQMessageBus(options))
            await provision.SubscribeAsync<IMessage>(_ => { }, token);
        string id = Guid.NewGuid().ToString("N");
        string directory = Path.Combine(Path.GetTempPath(), $"foundatio-delayed-process-{id}");
        Directory.CreateDirectory(directory);
        string checkpoint = Path.Combine(directory, "accepted.json");
        string helper = GetProcessProbePath();
        var start = new ProcessStartInfo("dotnet")
        {
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            CreateNoWindow = true
        };
        start.ArgumentList.Add(helper);
        // Do not put broker credentials in command-line arguments or test output.
        start.Environment["FOUNDATIO_PROBE_URI"] = fixture.MessagingDelayedConnectionString;
        start.Environment["FOUNDATIO_PROBE_TOPIC"] = context.Topic;
        start.Environment["FOUNDATIO_PROBE_ID"] = id;
        start.Environment["FOUNDATIO_PROBE_CHECKPOINT"] = checkpoint;
        using var process = Process.Start(start) ?? throw new InvalidOperationException("The test publisher process did not start.");
        var stdout = process.StandardOutput.ReadToEndAsync(token);
        var stderr = process.StandardError.ReadToEndAsync(token);
        try
        {
            while (!File.Exists(checkpoint))
            {
                Assert.False(process.HasExited, "The publisher exited before confirming its scheduled event.");
                await Task.Delay(TimeSpan.FromMilliseconds(50), token);
            }
            using var accepted = JsonDocument.Parse(await File.ReadAllTextAsync(checkpoint, token));
            Assert.Equal(id, accepted.RootElement.GetProperty("Id").GetString());
            var started = accepted.RootElement.GetProperty("StartedAt").GetDateTimeOffset();
            Assert.False(process.HasExited);
            // Act
            process.Kill(entireProcessTree: true);
            await process.WaitForExitAsync(token);
            // Assert
            Assert.True(process.HasExited);
            Assert.True(DateTimeOffset.UtcNow < started.AddSeconds(30), "The process must be killed before its delayed event is due.");
            _logger.LogInformation("Killed publisher after confirmed scheduling and before due time: event={MessageId}, process={ProcessId}",
                id, process.Id);

            var received = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
            await using var subscriber = new RabbitMQMessageBus(options);
            await subscriber.SubscribeAsync<IMessage>(message =>
            {
                Assert.Contains(id, Encoding.UTF8.GetString(message.Data.Span));
                received.TrySetResult(message.UniqueId!);
            }, token);
            Assert.Equal(id, await received.Task.WaitAsync(token));
            await RabbitMqReliabilityTestContext.WaitAsync(() => subscriber.ActiveDeliveryCount == 0, token);
            _logger.LogInformation("Broker delivered scheduled event after publisher process termination: {MessageId}",
                id);
        }
        finally
        {
            if (!process.HasExited)
            {
                process.Kill(entireProcessTree: true);
                using var cleanup = new CancellationTokenSource(TimeSpan.FromSeconds(10));
                await process.WaitForExitAsync(cleanup.Token);
            }
            try
            {
                _logger.LogInformation("Publisher standard output: {Output}", await stdout);
                _logger.LogInformation("Publisher standard error: {Error}", await stderr);
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
            }
            Directory.Delete(directory, recursive: true);
        }
    }

    private static string GetProcessProbePath()
    {
        var testOutput = new DirectoryInfo(AppContext.BaseDirectory);
        string configuration = testOutput.Parent?.Name
            ?? throw new InvalidOperationException("Cannot locate the test build configuration.");
        DirectoryInfo? root = testOutput;
        while (root is not null && !File.Exists(Path.Combine(root.FullName, "Foundatio.RabbitMQ.slnx")))
            root = root.Parent;
        Assert.NotNull(root);
        string helper = Path.Combine(root.FullName, "tests", "Foundatio.RabbitMQ.ProcessProbe", "bin", configuration,
            "net10.0", "Foundatio.RabbitMQ.ProcessProbe.dll");
        Assert.True(File.Exists(helper), "Build the solution, including its process probe, before running integration tests.");
        return helper;
    }
}
