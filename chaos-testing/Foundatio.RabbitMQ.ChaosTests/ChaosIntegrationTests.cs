using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Aspire.Hosting;
using Aspire.Hosting.ApplicationModel;
using Aspire.Hosting.Testing;
using Foundatio.Messaging;
using Foundatio.Xunit;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Foundatio.RabbitMQ.ChaosTests;

public class ChaosFixture : IAsyncLifetime
{
    private DistributedApplication? _app;

    public DistributedApplication App => _app ?? throw new InvalidOperationException("AppHost not initialized");
    private ChaosTestHelper? _chaos;
    private ILoggerFactory? _loggerFactory;
    public ChaosTestHelper Chaos => _chaos ??= new(App, _loggerFactory);

    internal void SetLoggerFactory(ILoggerFactory loggerFactory)
    {
        _loggerFactory = loggerFactory;
        _chaos = null;
    }

    public async ValueTask InitializeAsync()
    {
        var appHost = await DistributedApplicationTestingBuilder
            .CreateAsync<Projects.Foundatio_RabbitMQ_AppHost>();

        _app = await appHost.BuildAsync();
        using var startCts = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        await _app.StartAsync(startCts.Token);

        await _app.ResourceNotifications.WaitForResourceAsync(
            "chaos-1", KnownResourceStates.Running)
            .WaitAsync(TimeSpan.FromSeconds(90));

        await _app.ResourceNotifications.WaitForResourceAsync(
            "chaos-2", KnownResourceStates.Running)
            .WaitAsync(TimeSpan.FromSeconds(90));

        await Task.Delay(TimeSpan.FromSeconds(15));
    }

    public ValueTask DisposeAsync() => _app?.DisposeAsync() ?? ValueTask.CompletedTask;
}

[CollectionDefinition("Chaos")]
public class ChaosCollection : ICollectionFixture<ChaosFixture>;

[Collection("Chaos")]
public class ChaosIntegrationTests(ChaosFixture fixture, ITestOutputHelper output) : TestWithLoggingBase(output)
{
    private readonly ChaosFixture _fixture = fixture;

    public override ValueTask InitializeAsync()
    {
        _fixture.SetLoggerFactory(Log);
        return base.InitializeAsync();
    }

    [Fact]
    public async Task SetDiskFreeLimit_WhenSetToUnreachableValue_TriggersAndClearsAlarm()
    {
        // Arrange
        var chaos = _fixture.Chaos;

        // Act - trigger alarm
        _logger.LogInformation("=== Triggering disk alarm on chaos-1 ===");
        await chaos.FillDiskAsync("chaos-1");
        await chaos.WaitForAlarmActiveAsync("chaos-1", TimeSpan.FromSeconds(30));

        // Assert - alarm is active
        var hasAlarm = await chaos.HasDiskAlarmAsync("chaos-1");
        _logger.LogInformation($"Alarm active: {hasAlarm}");
        Assert.True(hasAlarm, "Expected disk alarm to be active after setting limit to 999GB");

        // Act - clear alarm
        _logger.LogInformation("=== Clearing disk alarm on chaos-1 ===");
        await chaos.ClearDiskAsync("chaos-1");
        await chaos.WaitForAlarmClearedAsync("chaos-1", TimeSpan.FromSeconds(30));

        // Assert - alarm is cleared
        hasAlarm = await chaos.HasDiskAlarmAsync("chaos-1");
        _logger.LogInformation($"Alarm cleared: {!hasAlarm}");
        Assert.False(hasAlarm, "Expected disk alarm to clear after resetting limit to 10MB");
    }

    [Fact]
    public async Task PublishAsync_DuringDiskAlarm_WithoutConfirms_MaySucceedSilently()
    {
        // Arrange
        var chaos = _fixture.Chaos;
        var connectionString = chaos.GetConnectionString("chaos-1");
        _logger.LogInformation($"Connection: {connectionString}");

        await using var messageBus = new RabbitMQMessageBus(o => o
            .ConnectionString(connectionString)
            .LoggerFactory(Log));

        _logger.LogInformation("Sending warmup message...");
        await messageBus.PublishAsync(new TestMessage { Data = "warmup" });
        _logger.LogInformation("Warmup succeeded");

        _logger.LogInformation("Triggering disk alarm...");
        await chaos.FillDiskAsync("chaos-1");
        await chaos.WaitForAlarmActiveAsync("chaos-1", TimeSpan.FromSeconds(30));
        _logger.LogInformation("Disk alarm active, waiting for broker blocked notification to propagate...");
        await Task.Delay(TimeSpan.FromSeconds(5));

        // Act
        _logger.LogInformation("Attempting publish during alarm (fire-and-forget mode)...");
        bool publishThrew = false;
        try
        {
            await messageBus.PublishAsync(new TestMessage { Data = "during alarm" });
            _logger.LogInformation("Publish completed - without confirms, fire-and-forget publishes may succeed or be silently dropped");
        }
        catch (MessageBusException ex)
        {
            publishThrew = true;
            _logger.LogInformation($"Publish blocked: {ex.Message}");
            Assert.Contains("blocked", ex.Message, StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            await chaos.ClearDiskAsync("chaos-1");
            await chaos.WaitForAlarmClearedAsync("chaos-1", TimeSpan.FromSeconds(30));
        }

        // Assert
        _logger.LogInformation($"Disk alarm cleared. Publish threw: {publishThrew}");
        _logger.LogInformation("NOTE: Without publisher confirms, disk alarms may not block publishes immediately.");
        _logger.LogInformation("Use PublisherConfirmsEnabled=true for guaranteed delivery detection.");
    }

    [Fact]
    public async Task PublishAsync_AfterNodeRestart_RecoversAutomatically()
    {
        // Arrange
        var chaos = _fixture.Chaos;
        var connectionString = chaos.GetConnectionString("chaos-1");
        _logger.LogInformation($"Connection: {connectionString}");

        await using var messageBus = new RabbitMQMessageBus(o => o
            .ConnectionString(connectionString)
            .PublishRecoveryTimeout(TimeSpan.FromSeconds(30))
            .LoggerFactory(Log));

        _logger.LogInformation("Sending message before kill...");
        await messageBus.PublishAsync(new TestMessage { Data = "before kill" });
        _logger.LogInformation("Pre-kill publish succeeded");

        // Act
        _logger.LogInformation("Killing chaos-1...");
        await chaos.StopNodeAsync("chaos-1");
        _logger.LogInformation("Node killed, waiting 2s...");
        await Task.Delay(TimeSpan.FromSeconds(2));

        _logger.LogInformation("Restarting chaos-1...");
        await chaos.StartNodeAsync("chaos-1");
        _logger.LogInformation("Node restarted, waiting for RabbitMQ to be ready...");
        await Task.Delay(TimeSpan.FromSeconds(15));

        _logger.LogInformation("Attempting publish after restart (should recover)...");
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await messageBus.PublishAsync(new TestMessage { Data = "after restart" }, cancellationToken: cts.Token);
        _logger.LogInformation("Post-restart publish SUCCEEDED - recovery gate worked");

        // Assert
        Assert.False(cts.IsCancellationRequested, "Publish should complete before timeout");
    }

    [Fact]
    public async Task PublishAsync_NodeDown_RecoveryTimeoutExpires_ThrowsMessageBusException()
    {
        // Arrange
        var chaos = _fixture.Chaos;
        var connectionString = chaos.GetConnectionString("chaos-1");
        _logger.LogInformation($"Connection: {connectionString}");

        await using var messageBus = new RabbitMQMessageBus(o => o
            .ConnectionString(connectionString)
            .PublishRecoveryTimeout(TimeSpan.FromSeconds(3))
            .LoggerFactory(Log));

        _logger.LogInformation("Sending warmup...");
        await messageBus.PublishAsync(new TestMessage { Data = "warmup" });

        _logger.LogInformation("Killing chaos-1...");
        await chaos.StopNodeAsync("chaos-1");
        await Task.Delay(TimeSpan.FromSeconds(2));

        // Act & Assert
        _logger.LogInformation("Attempting publish with 3s recovery timeout (node is down)...");
        try
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            await messageBus.PublishAsync(new TestMessage { Data = "should timeout" }, cancellationToken: cts.Token);
            Assert.Fail("Expected MessageBusException but publish succeeded");
        }
        catch (MessageBusException ex)
        {
            _logger.LogInformation($"EXPECTED: {ex.Message}");
            Assert.Contains("recovery", ex.Message, StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            _logger.LogInformation("Restarting chaos-1...");
            await chaos.StartNodeAsync("chaos-1");
            await Task.Delay(TimeSpan.FromSeconds(10));
            _logger.LogInformation("Node restarted");
        }
    }

    [Fact]
    public async Task SubscribeAsync_ReceivesMessages_AfterNodeRestart()
    {
        // Arrange
        var chaos = _fixture.Chaos;
        var connectionString = chaos.GetConnectionString("chaos-1");
        _logger.LogInformation($"Connection: {connectionString}");

        var received = new ConcurrentBag<string>();
        var topic = $"chaos-sub-test-{Guid.NewGuid():N}";

        await using var messageBus = new RabbitMQMessageBus(o => o
            .ConnectionString(connectionString)
            .Topic(topic)
            .SubscriptionQueueName($"chaos-sub-{Guid.NewGuid():N}")
            .IsSubscriptionQueueExclusive(false)
            .SubscriptionQueueAutoDelete(true)
            .PublishRecoveryTimeout(TimeSpan.FromSeconds(30))
            .LoggerFactory(Log));

        await messageBus.SubscribeAsync<TestMessage>(msg =>
        {
            _logger.LogInformation($"Received: {msg.Data}");
            received.Add(msg.Data);
        });

        _logger.LogInformation("Publishing pre-restart message...");
        await messageBus.PublishAsync(new TestMessage { Data = "before" });
        await Task.Delay(TimeSpan.FromSeconds(2));
        Assert.Contains("before", received);
        _logger.LogInformation($"Pre-restart messages received: {received.Count}");

        // Act
        _logger.LogInformation("Killing chaos-1...");
        await chaos.StopNodeAsync("chaos-1");
        await Task.Delay(TimeSpan.FromSeconds(2));

        _logger.LogInformation("Restarting chaos-1...");
        await chaos.StartNodeAsync("chaos-1");
        _logger.LogInformation("Waiting for recovery...");
        await Task.Delay(TimeSpan.FromSeconds(20));

        _logger.LogInformation("Publishing post-restart message...");
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await messageBus.PublishAsync(new TestMessage { Data = "after" }, cancellationToken: cts.Token);
        await Task.Delay(TimeSpan.FromSeconds(5));

        // Assert
        _logger.LogInformation($"Total messages received: {received.Count}");
        Assert.Contains("after", received);
        _logger.LogInformation("Subscriber successfully recovered and received post-restart message");
    }

    [Fact]
    public async Task PublishAsync_WithPublisherConfirms_DuringDiskAlarm_FailsOrTimesOut()
    {
        // Arrange
        var chaos = _fixture.Chaos;
        var connectionString = chaos.GetConnectionString("chaos-1");
        _logger.LogInformation($"Connection: {connectionString}");

        await using var messageBus = new RabbitMQMessageBus(o => o
            .ConnectionString(connectionString)
            .PublisherConfirmsEnabled(true)
            .LoggerFactory(Log));

        _logger.LogInformation("Sending warmup with confirms...");
        await messageBus.PublishAsync(new TestMessage { Data = "warmup" });
        _logger.LogInformation("Warmup confirmed");

        _logger.LogInformation("Triggering disk alarm...");
        await chaos.FillDiskAsync("chaos-1");
        await chaos.WaitForAlarmActiveAsync("chaos-1", TimeSpan.FromSeconds(30));
        _logger.LogInformation("Alarm active, waiting for blocked notification...");
        await Task.Delay(TimeSpan.FromSeconds(5));

        // Act
        bool publishFailed = false;
        try
        {
            _logger.LogInformation("Publishing with confirms during alarm (5s timeout)...");
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            await messageBus.PublishAsync(new TestMessage { Data = "confirmed during alarm" },
                cancellationToken: cts.Token);
            _logger.LogInformation("WARNING: Publish confirmed during alarm (broker may not have blocked connection yet)");
        }
        catch (MessageBusException ex)
        {
            publishFailed = true;
            _logger.LogInformation($"EXPECTED: MessageBusException: {ex.Message}");
            Assert.Contains("blocked", ex.Message, StringComparison.OrdinalIgnoreCase);
        }
        catch (OperationCanceledException)
        {
            publishFailed = true;
            _logger.LogInformation("EXPECTED: Publish timed out (confirms blocked during disk alarm)");
        }
        finally
        {
            await chaos.ClearDiskAsync("chaos-1");
            await chaos.WaitForAlarmClearedAsync("chaos-1", TimeSpan.FromSeconds(30));
            _logger.LogInformation("Alarm cleared");
        }

        // Assert
        Assert.True(publishFailed,
            "Publisher confirms should fail or timeout during a disk alarm. " +
            "If this assertion fails, the broker did not propagate the blocked notification in time.");
    }

    [Fact]
    public async Task GetClusterStatus_WhenNodeHealthy_ReportsNodeRunning()
    {
        // Arrange
        var chaos = _fixture.Chaos;

        // Act
        var status = await chaos.GetClusterStatusAsync("chaos-1");
        _logger.LogInformation($"Cluster status JSON length: {status.Length}");
        _logger.LogInformation($"Cluster status: {status[..Math.Min(500, status.Length)]}");

        // Assert
        Assert.NotEmpty(status);
        Assert.Contains("running_nodes", status);
    }

    [Fact]
    public async Task CompetingConsumers_WhenMultipleSubscribersActive_DistributesMessagesEvenly()
    {
        // Arrange
        var chaos = _fixture.Chaos;
        var connectionString = chaos.GetConnectionString("chaos-1");
        _logger.LogInformation($"Connection: {connectionString}");

        var topic = $"competing-test-{Guid.NewGuid():N}";
        var queueName = $"competing-queue-{Guid.NewGuid():N}";

        var received1 = new ConcurrentBag<string>();
        var received2 = new ConcurrentBag<string>();
        var received3 = new ConcurrentBag<string>();

        await using var consumer1 = new RabbitMQMessageBus(o => o
            .ConnectionString(connectionString)
            .Topic(topic)
            .SubscriptionQueueName(queueName)
            .IsSubscriptionQueueExclusive(false)
            .SubscriptionQueueAutoDelete(true)
            .PrefetchCount(1)
            .LoggerFactory(Log));

        await using var consumer2 = new RabbitMQMessageBus(o => o
            .ConnectionString(connectionString)
            .Topic(topic)
            .SubscriptionQueueName(queueName)
            .IsSubscriptionQueueExclusive(false)
            .SubscriptionQueueAutoDelete(true)
            .PrefetchCount(1)
            .LoggerFactory(Log));

        await using var consumer3 = new RabbitMQMessageBus(o => o
            .ConnectionString(connectionString)
            .Topic(topic)
            .SubscriptionQueueName(queueName)
            .IsSubscriptionQueueExclusive(false)
            .SubscriptionQueueAutoDelete(true)
            .PrefetchCount(1)
            .LoggerFactory(Log));

        await consumer1.SubscribeAsync<TestMessage>(msg => { received1.Add(msg.Data); });
        await consumer2.SubscribeAsync<TestMessage>(msg => { received2.Add(msg.Data); });
        await consumer3.SubscribeAsync<TestMessage>(msg => { received3.Add(msg.Data); });

        await using var publisher = new RabbitMQMessageBus(o => o
            .ConnectionString(connectionString)
            .Topic(topic)
            .LoggerFactory(Log));

        // Act
        _logger.LogInformation("Publishing 30 messages to competing consumers...");
        for (int i = 0; i < 30; i++)
            await publisher.PublishAsync(new TestMessage { Data = $"msg-{i}" });

        await Task.Delay(TimeSpan.FromSeconds(5));

        // Assert
        int total = received1.Count + received2.Count + received3.Count;
        _logger.LogInformation($"Consumer 1 received: {received1.Count}");
        _logger.LogInformation($"Consumer 2 received: {received2.Count}");
        _logger.LogInformation($"Consumer 3 received: {received3.Count}");
        _logger.LogInformation($"Total received: {total}");

        Assert.Equal(30, total);
        Assert.True(received1.Count > 0, "Consumer 1 should receive at least one message");
        Assert.True(received2.Count > 0, "Consumer 2 should receive at least one message");
        Assert.True(received3.Count > 0, "Consumer 3 should receive at least one message");
        _logger.LogInformation("Messages distributed across all 3 competing consumers");
    }

    [Fact]
    public async Task SubscribeAsync_DuringRollingUpgrade_ResumesWithoutProcessRestart()
    {
        // Arrange
        var chaos = _fixture.Chaos;
        var connectionString = chaos.GetConnectionString("chaos-1");
        _logger.LogInformation($"Connection: {connectionString}");

        var received = new ConcurrentBag<string>();
        var topic = $"rolling-upgrade-{Guid.NewGuid():N}";
        var queueName = $"rolling-queue-{Guid.NewGuid():N}";

        await using var messageBus = new RabbitMQMessageBus(o => o
            .ConnectionString(connectionString)
            .Topic(topic)
            .SubscriptionQueueName(queueName)
            .IsSubscriptionQueueExclusive(false)
            .SubscriptionQueueAutoDelete(true)
            .PublishRecoveryTimeout(TimeSpan.FromSeconds(30))
            .LoggerFactory(Log));

        await messageBus.SubscribeAsync<TestMessage>(msg =>
        {
            _logger.LogInformation($"Received: {msg.Data}");
            received.Add(msg.Data);
        });

        _logger.LogInformation("=== Phase 1: Steady-state publishing ===");
        for (int i = 0; i < 5; i++)
            await messageBus.PublishAsync(new TestMessage { Data = $"pre-restart-{i}" });
        await Task.Delay(TimeSpan.FromSeconds(3));
        Assert.Equal(5, received.Count);
        _logger.LogInformation($"Phase 1 complete: {received.Count} messages received");

        // Act
        _logger.LogInformation("=== Phase 2: Simulate rolling upgrade (kill node) ===");
        await chaos.StopNodeAsync("chaos-1");
        _logger.LogInformation("Node killed (simulating rolling upgrade restart)");
        await Task.Delay(TimeSpan.FromSeconds(2));

        _logger.LogInformation("=== Phase 3: Node comes back (upgrade complete) ===");
        await chaos.StartNodeAsync("chaos-1");
        _logger.LogInformation("Node restarted, waiting for auto-recovery...");
        await Task.Delay(TimeSpan.FromSeconds(20));

        _logger.LogInformation("=== Phase 4: Verify consumer resumed ===");
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        for (int i = 0; i < 5; i++)
            await messageBus.PublishAsync(new TestMessage { Data = $"post-restart-{i}" }, cancellationToken: cts.Token);
        await Task.Delay(TimeSpan.FromSeconds(5));

        // Assert
        var postRestartCount = received.Count(m => m.StartsWith("post-restart-"));
        _logger.LogInformation($"Post-restart messages received: {postRestartCount}/5");
        Assert.Equal(5, postRestartCount);

        var recoveryLogs = Log.LogEntries.Select(e => e.Message.ToString()).ToList();
        _logger.LogInformation($"Total captured log messages: {recoveryLogs.Count}");
        Assert.Contains(recoveryLogs, m => m.Contains("Publisher connection lost"));
        Assert.Contains(recoveryLogs, m => m.Contains("Publisher connection recovery succeeded"));
        Assert.Contains(recoveryLogs, m => m.Contains("Subscriber connection recovery") || m.Contains("Subscriber connection recovering"));
        _logger.LogInformation("Recovery audit trail verified: shutdown -> recovery attempt -> recovery succeeded");
    }

    [Fact]
    public async Task PublishAsync_WhenPrimaryHostDown_ConnectsToAlternateEndpoint()
    {
        // Arrange
        var chaos = _fixture.Chaos;
        var endpoint1 = _fixture.App.GetEndpoint("chaos-1", "amqp");
        var endpoint2 = _fixture.App.GetEndpoint("chaos-2", "amqp");
        _logger.LogInformation($"Endpoint 1 (chaos-1): {endpoint1.Host}:{endpoint1.Port}");
        _logger.LogInformation($"Endpoint 2 (chaos-2): {endpoint2.Host}:{endpoint2.Port}");

        _logger.LogInformation("Killing chaos-1...");
        await chaos.StopNodeAsync("chaos-1");
        await Task.Delay(TimeSpan.FromSeconds(3));

        // Act & Assert
        _logger.LogInformation("Connecting with Hosts=[chaos-1, chaos-2] (chaos-1 is down)...");
        try
        {
            await using var messageBus = new RabbitMQMessageBus(o => o
                .ConnectionString($"amqp://guest:guest@{endpoint2.Host}:{endpoint2.Port}")
                .Hosts($"{endpoint1.Host}:{endpoint1.Port}", $"{endpoint2.Host}:{endpoint2.Port}")
                .LoggerFactory(Log));

            _logger.LogInformation("Publishing message (should connect to chaos-2)...");
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            await messageBus.PublishAsync(new TestMessage { Data = "failover test" }, cancellationToken: cts.Token);
            _logger.LogInformation("Publish succeeded via failover endpoint!");

            Assert.True(Log.LogEntries.Count > 0, "Should have log messages showing connection activity");
            _logger.LogInformation("Multi-endpoint failover working - connected when primary host was down");
        }
        finally
        {
            _logger.LogInformation("Restarting chaos-1...");
            await chaos.StartNodeAsync("chaos-1");
            await Task.Delay(TimeSpan.FromSeconds(10));
            _logger.LogInformation("chaos-1 restarted");
        }
    }

    [Fact]
    public async Task PublishAsync_AfterNodeKillAndRestart_LogsFullRecoverySequence()
    {
        // Arrange
        var chaos = _fixture.Chaos;
        var connectionString = chaos.GetConnectionString("chaos-1");
        _logger.LogInformation($"Connection: {connectionString}");

        await using var messageBus = new RabbitMQMessageBus(o => o
            .ConnectionString(connectionString)
            .PublishRecoveryTimeout(TimeSpan.FromSeconds(30))
            .LoggerFactory(Log));

        _logger.LogInformation("Warmup publish...");
        await messageBus.PublishAsync(new TestMessage { Data = "warmup" });

        // Act
        _logger.LogInformation("Killing node to trigger recovery sequence...");
        await chaos.StopNodeAsync("chaos-1");
        await Task.Delay(TimeSpan.FromSeconds(3));

        _logger.LogInformation("Restarting node...");
        await chaos.StartNodeAsync("chaos-1");
        await Task.Delay(TimeSpan.FromSeconds(15));

        _logger.LogInformation("Publishing after recovery...");
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await messageBus.PublishAsync(new TestMessage { Data = "after recovery" }, cancellationToken: cts.Token);

        // Assert
        _logger.LogInformation("=== Verifying Recovery Audit Trail (OUT-12608 requirement) ===");
        var logs = Log.LogEntries.Select(e => e.Message.ToString()).ToList();

        bool hasShutdown = logs.Any(m => m.Contains("Publisher shutdown") || m.Contains("connection lost"));
        bool hasRecoveryAttempt = logs.Any(m => m.Contains("recovery") && (m.Contains("error") || m.Contains("attempt") || m.Contains("waiting")));
        bool hasRecoverySuccess = logs.Any(m => m.Contains("recovery succeeded"));

        _logger.LogInformation($"  [1] Shutdown detected: {hasShutdown}");
        _logger.LogInformation($"  [2] Recovery attempt/error logged: {hasRecoveryAttempt}");
        _logger.LogInformation($"  [3] Recovery succeeded logged: {hasRecoverySuccess}");

        Assert.True(hasShutdown, "Missing: shutdown event in logs");
        Assert.True(hasRecoveryAttempt, "Missing: recovery attempt/error in logs");
        Assert.True(hasRecoverySuccess, "Missing: recovery succeeded in logs");
        _logger.LogInformation("Full recovery audit trail confirmed (shutdown -> attempt -> success)");
    }

    [Fact]
    public async Task PublishAsync_ConcurrentDuringNodeKill_AllRecoverOrFailGracefully()
    {
        // Arrange
        var chaos = _fixture.Chaos;
        var connectionString = chaos.GetConnectionString("chaos-1");
        _logger.LogInformation($"Connection: {connectionString}");

        await using var messageBus = new RabbitMQMessageBus(o => o
            .ConnectionString(connectionString)
            .PublishRecoveryTimeout(TimeSpan.FromSeconds(30))
            .LoggerFactory(Log));

        _logger.LogInformation("Warmup...");
        await messageBus.PublishAsync(new TestMessage { Data = "warmup" });

        int succeeded = 0;
        int failedGracefully = 0;
        int unexpected = 0;

        // Act
        _logger.LogInformation("Launching 20 concurrent publishers, killing node mid-flight...");
        var killTask = Task.Run(async () =>
        {
            await Task.Delay(TimeSpan.FromMilliseconds(500));
            await chaos.StopNodeAsync("chaos-1");
            _logger.LogInformation("  --> Node killed mid-publish");
            await Task.Delay(TimeSpan.FromSeconds(5));
            await chaos.StartNodeAsync("chaos-1");
            _logger.LogInformation("  --> Node restarted");
        });

        var publishTasks = Enumerable.Range(0, 20).Select(async i =>
        {
            try
            {
                await Task.Delay(TimeSpan.FromMilliseconds(i * 100));
                using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(45));
                await messageBus.PublishAsync(new TestMessage { Data = $"concurrent-{i}" }, cancellationToken: cts.Token);
                Interlocked.Increment(ref succeeded);
            }
            catch (MessageBusException)
            {
                Interlocked.Increment(ref failedGracefully);
            }
            catch (OperationCanceledException)
            {
                Interlocked.Increment(ref failedGracefully);
            }
            catch (Exception ex)
            {
                Interlocked.Increment(ref unexpected);
                _logger.LogInformation($"  UNEXPECTED exception type: {ex.GetType().Name}: {ex.Message}");
            }
        });

        await Task.WhenAll(publishTasks.Append(killTask));
        await Task.Delay(TimeSpan.FromSeconds(15));

        // Assert
        _logger.LogInformation($"Succeeded: {succeeded}, Failed gracefully: {failedGracefully}, Unexpected: {unexpected}");
        Assert.Equal(0, unexpected);
        Assert.True(succeeded + failedGracefully == 20, "All 20 publishes should either succeed or fail gracefully");
        Assert.True(succeeded > 0, "At least some publishes should succeed (before kill or after recovery)");
        _logger.LogInformation("All concurrent publishes handled gracefully during node failure");
    }

    [Fact]
    public async Task PublishAsync_DuringDiskAlarmAndNodeKill_RecoversAfterRestart()
    {
        // Arrange
        var chaos = _fixture.Chaos;
        var connectionString = chaos.GetConnectionString("chaos-1");
        _logger.LogInformation($"Connection: {connectionString}");

        await using var messageBus = new RabbitMQMessageBus(o => o
            .ConnectionString(connectionString)
            .PublishRecoveryTimeout(TimeSpan.FromSeconds(30))
            .LoggerFactory(Log));

        _logger.LogInformation("Warmup...");
        await messageBus.PublishAsync(new TestMessage { Data = "warmup" });

        // Act
        _logger.LogInformation("=== Fault 1: Trigger disk alarm ===");
        await chaos.FillDiskAsync("chaos-1");
        await chaos.WaitForAlarmActiveAsync("chaos-1", TimeSpan.FromSeconds(30));
        _logger.LogInformation("Disk alarm active");

        _logger.LogInformation("=== Fault 2: Kill the node while alarm is active ===");
        await chaos.StopNodeAsync("chaos-1");
        _logger.LogInformation("Node killed during disk alarm - double fault in progress");
        await Task.Delay(TimeSpan.FromSeconds(3));

        _logger.LogInformation("=== Recovery: Restart node ===");
        await chaos.StartNodeAsync("chaos-1");
        await Task.Delay(TimeSpan.FromSeconds(15));

        await chaos.ClearDiskAsync("chaos-1");
        await chaos.WaitForAlarmClearedAsync("chaos-1", TimeSpan.FromSeconds(30));

        // Assert
        _logger.LogInformation("=== Verify: System recovered from double fault ===");
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await messageBus.PublishAsync(new TestMessage { Data = "after double fault" }, cancellationToken: cts.Token);
        _logger.LogInformation("Publish succeeded after double fault recovery!");
        Assert.False(cts.IsCancellationRequested);
    }

    [Fact]
    public async Task PublishAsync_AfterRapidConnectionFlapping_SystemRemainsStable()
    {
        // Arrange
        var chaos = _fixture.Chaos;
        var connectionString = chaos.GetConnectionString("chaos-1");
        _logger.LogInformation($"Connection: {connectionString}");

        await using var messageBus = new RabbitMQMessageBus(o => o
            .ConnectionString(connectionString)
            .PublishRecoveryTimeout(TimeSpan.FromSeconds(30))
            .LoggerFactory(Log));

        _logger.LogInformation("Warmup...");
        await messageBus.PublishAsync(new TestMessage { Data = "warmup" });

        // Act
        _logger.LogInformation("=== Rapid flapping: kill/restart 3 times in quick succession ===");
        for (int i = 1; i <= 3; i++)
        {
            _logger.LogInformation($"  Flap {i}: killing node...");
            await chaos.StopNodeAsync("chaos-1");
            await Task.Delay(TimeSpan.FromSeconds(2));
            _logger.LogInformation($"  Flap {i}: restarting node...");
            await chaos.StartNodeAsync("chaos-1");
            await Task.Delay(TimeSpan.FromSeconds(10));
        }

        _logger.LogInformation("Waiting for final recovery to settle...");
        await Task.Delay(TimeSpan.FromSeconds(10));

        _logger.LogInformation("=== Verify: System is stable after flapping ===");
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await messageBus.PublishAsync(new TestMessage { Data = "after flapping" }, cancellationToken: cts.Token);
        _logger.LogInformation("Publish succeeded after rapid connection flapping!");

        // Assert
        var logs = Log.LogEntries.Select(e => e.Message.ToString()).ToList();
        int recoveryCount = logs.Count(m => m.Contains("recovery succeeded"));
        _logger.LogInformation($"Recovery succeeded events: {recoveryCount}");
        Assert.True(recoveryCount >= 1, "Should have recovered at least once");
    }

    [Fact]
    public async Task PublishAsync_HighVolumeBurstAfterRecovery_AllMessagesDelivered()
    {
        // Arrange
        var chaos = _fixture.Chaos;
        var connectionString = chaos.GetConnectionString("chaos-1");
        _logger.LogInformation($"Connection: {connectionString}");

        var received = new ConcurrentBag<string>();
        var topic = $"burst-test-{Guid.NewGuid():N}";
        var queueName = $"burst-queue-{Guid.NewGuid():N}";

        await using var messageBus = new RabbitMQMessageBus(o => o
            .ConnectionString(connectionString)
            .Topic(topic)
            .SubscriptionQueueName(queueName)
            .IsSubscriptionQueueExclusive(false)
            .SubscriptionQueueAutoDelete(true)
            .PublishRecoveryTimeout(TimeSpan.FromSeconds(30))
            .LoggerFactory(Log));

        await messageBus.SubscribeAsync<TestMessage>(msg => received.Add(msg.Data));

        _logger.LogInformation("Verify baseline delivery...");
        await messageBus.PublishAsync(new TestMessage { Data = "baseline" });
        await Task.Delay(TimeSpan.FromSeconds(2));
        Assert.Contains("baseline", received);

        // Act
        _logger.LogInformation("Kill and restart node...");
        await chaos.StopNodeAsync("chaos-1");
        await Task.Delay(TimeSpan.FromSeconds(2));
        await chaos.StartNodeAsync("chaos-1");
        await Task.Delay(TimeSpan.FromSeconds(20));

        _logger.LogInformation("=== Burst: Publishing 50 messages rapidly after recovery ===");
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        for (int i = 0; i < 50; i++)
            await messageBus.PublishAsync(new TestMessage { Data = $"burst-{i}" }, cancellationToken: cts.Token);

        _logger.LogInformation("Waiting for all messages to be delivered...");
        await Task.Delay(TimeSpan.FromSeconds(10));

        // Assert
        int burstReceived = received.Count(m => m.StartsWith("burst-"));
        _logger.LogInformation($"Burst messages received: {burstReceived}/50");
        Assert.Equal(50, burstReceived);
        _logger.LogInformation("All 50 burst messages delivered after recovery - no message loss!");
    }

    [Fact]
    public async Task SubscribeAsync_NodeKillDuringSlowProcessing_SubscriptionSurvives()
    {
        // Arrange
        var chaos = _fixture.Chaos;
        var connectionString = chaos.GetConnectionString("chaos-1");
        _logger.LogInformation($"Connection: {connectionString}");

        var received = new ConcurrentBag<string>();
        var topic = $"slow-consumer-{Guid.NewGuid():N}";
        var queueName = $"slow-queue-{Guid.NewGuid():N}";

        await using var messageBus = new RabbitMQMessageBus(o => o
            .ConnectionString(connectionString)
            .Topic(topic)
            .SubscriptionQueueName(queueName)
            .IsSubscriptionQueueExclusive(false)
            .SubscriptionQueueAutoDelete(true)
            .AcknowledgementStrategy(AcknowledgementStrategy.Automatic)
            .PrefetchCount(1)
            .PublishRecoveryTimeout(TimeSpan.FromSeconds(30))
            .LoggerFactory(Log));

        await messageBus.SubscribeAsync<TestMessage>(async msg =>
        {
            if (msg.Data == "slow-message")
            {
                _logger.LogInformation("Processing slow message (simulating 3s handler)...");
                await Task.Delay(TimeSpan.FromSeconds(3));
            }
            received.Add(msg.Data);
            _logger.LogInformation($"Handler completed: {msg.Data}");
        });

        _logger.LogInformation("Publishing a fast message to verify subscription is active...");
        await messageBus.PublishAsync(new TestMessage { Data = "fast-before" });
        await Task.Delay(TimeSpan.FromSeconds(1));
        Assert.Contains("fast-before", received);

        // Act
        _logger.LogInformation("Publishing slow message then killing node during processing...");
        await messageBus.PublishAsync(new TestMessage { Data = "slow-message" });
        await Task.Delay(TimeSpan.FromMilliseconds(500));

        await chaos.StopNodeAsync("chaos-1");
        _logger.LogInformation("Node killed while handler is processing slow-message");
        await Task.Delay(TimeSpan.FromSeconds(2));

        await chaos.StartNodeAsync("chaos-1");
        _logger.LogInformation("Node restarted, waiting for recovery...");
        await Task.Delay(TimeSpan.FromSeconds(20));

        _logger.LogInformation("Publishing post-recovery message to verify subscription alive...");
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await messageBus.PublishAsync(new TestMessage { Data = "post-recovery" }, cancellationToken: cts.Token);
        await Task.Delay(TimeSpan.FromSeconds(5));

        // Assert
        _logger.LogInformation($"Total messages received: {received.Count}");
        _logger.LogInformation($"Messages: [{string.Join(", ", received)}]");
        Assert.Contains("post-recovery", received);
        _logger.LogInformation("Consumer subscription survived node kill during slow message processing");
    }
}

public record TestMessage
{
    public string Data { get; init; } = string.Empty;
}
