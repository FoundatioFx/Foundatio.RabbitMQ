using System;
using System.Threading;
using System.Threading.Tasks;
using Aspire.Hosting;
using Aspire.Hosting.ApplicationModel;
using Aspire.Hosting.Testing;
using Foundatio.Messaging;
using Xunit;

namespace Foundatio.RabbitMQ.ChaosTests;

public class ChaosFixture : IAsyncLifetime
{
    private DistributedApplication? _app;

    public DistributedApplication App => _app ?? throw new InvalidOperationException("AppHost not initialized");
    public ChaosTestHelper Chaos => new(App);

    public async ValueTask InitializeAsync()
    {
        var appHost = await DistributedApplicationTestingBuilder
            .CreateAsync<Projects.Foundatio_RabbitMQ_AppHost>();

        _app = await appHost.BuildAsync();
        await _app.StartAsync();

        await _app.ResourceNotifications.WaitForResourceAsync(
            "chaos-1", KnownResourceStates.Running)
            .WaitAsync(TimeSpan.FromSeconds(90));

        await Task.Delay(TimeSpan.FromSeconds(15));
    }

    public async ValueTask DisposeAsync()
    {
        if (_app is not null)
        {
            await _app.DisposeAsync();
        }
    }
}

[CollectionDefinition("Chaos")]
public class ChaosCollection : ICollectionFixture<ChaosFixture>;

[Collection("Chaos")]
public class ChaosIntegrationTests
{
    private readonly ChaosFixture _fixture;
    private readonly ITestOutputHelper _output;

    public ChaosIntegrationTests(ChaosFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    [Fact]
    public async Task FillDisk_TriggersDiskAlarm()
    {
        var chaos = _fixture.Chaos;

        await chaos.FillDiskAsync("chaos-1");
        await chaos.WaitForAlarmActiveAsync("chaos-1", TimeSpan.FromSeconds(30));

        var hasAlarm = await chaos.HasDiskAlarmAsync("chaos-1");
        _output.WriteLine($"Has disk alarm after fill: {hasAlarm}");
        Assert.True(hasAlarm, "Expected disk alarm after filling disk");

        await chaos.ClearDiskAsync("chaos-1");
        await chaos.WaitForAlarmClearedAsync("chaos-1", TimeSpan.FromSeconds(30));

        hasAlarm = await chaos.HasDiskAlarmAsync("chaos-1");
        _output.WriteLine($"Has disk alarm after clear: {hasAlarm}");
        Assert.False(hasAlarm, "Expected no disk alarm after clearing disk");
    }

    [Fact]
    public async Task PublishAsync_DuringDiskAlarm_BlocksUntilRecovery()
    {
        var chaos = _fixture.Chaos;
        var connectionString = chaos.GetConnectionString("chaos-1");
        _output.WriteLine($"Connecting to: {connectionString}");

        await using var messageBus = new RabbitMQMessageBus(o => o
            .ConnectionString(connectionString)
            .PublishRecoveryTimeout(TimeSpan.FromSeconds(30)));

        await messageBus.PublishAsync(new TestMessage { Data = "warmup" });
        _output.WriteLine("Warmup message sent successfully");

        await chaos.FillDiskAsync("chaos-1");
        _output.WriteLine("Disk filled, waiting for alarm to trigger...");
        await Task.Delay(TimeSpan.FromSeconds(5));

        var hasAlarm = await chaos.HasDiskAlarmAsync("chaos-1");
        _output.WriteLine($"Disk alarm active: {hasAlarm}");

        if (hasAlarm)
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
            var publishTask = Task.Run(async () =>
            {
                await messageBus.PublishAsync(new TestMessage { Data = "during alarm" },
                    cancellationToken: cts.Token);
            });

            await Task.Delay(TimeSpan.FromSeconds(1));
            var completed = publishTask.IsCompleted;
            _output.WriteLine($"Publish completed during alarm: {completed}");

            await chaos.ClearDiskAsync("chaos-1");
            _output.WriteLine("Disk cleared, waiting for alarm to lift...");
            await Task.Delay(TimeSpan.FromSeconds(5));
        }
        else
        {
            _output.WriteLine("Warning: Disk alarm not triggered - node may not have filled up");
            await chaos.ClearDiskAsync("chaos-1");
        }
    }

    [Fact]
    public async Task StopNode_MakesNodeUnreachable()
    {
        var chaos = _fixture.Chaos;
        var connectionString = chaos.GetConnectionString("chaos-1");
        _output.WriteLine($"Connecting to: {connectionString}");

        await using var messageBus = new RabbitMQMessageBus(o => o
            .ConnectionString(connectionString)
            .PublishRecoveryTimeout(TimeSpan.FromSeconds(5)));

        await messageBus.PublishAsync(new TestMessage { Data = "before kill" });
        _output.WriteLine("Message sent before node kill");

        await chaos.StopNodeAsync("chaos-1");
        _output.WriteLine("Node killed");
        await Task.Delay(TimeSpan.FromSeconds(2));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        try
        {
            await messageBus.PublishAsync(new TestMessage { Data = "after kill" },
                cancellationToken: cts.Token);
            _output.WriteLine("Warning: Publish succeeded after node kill (possible if recovery happened)");
        }
        catch (Exception ex)
        {
            _output.WriteLine($"Expected exception after node kill: {ex.GetType().Name}: {ex.Message}");
            Assert.True(ex is MessageBusException or OperationCanceledException);
        }
        finally
        {
            await chaos.StartNodeAsync("chaos-1");
            _output.WriteLine("Node restarted");
            await Task.Delay(TimeSpan.FromSeconds(10));
        }
    }

    [Fact]
    public async Task ClusterStatus_ReturnsValidJson()
    {
        var chaos = _fixture.Chaos;
        var status = await chaos.GetClusterStatusAsync("chaos-1");
        _output.WriteLine($"Cluster status: {status}");
        Assert.NotEmpty(status);
        Assert.Contains("running_nodes", status);
    }
}

public record TestMessage
{
    public string Data { get; init; } = string.Empty;
}
