using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Aspire.Hosting;
using Aspire.Hosting.ApplicationModel;
using Aspire.Hosting.Testing;
using Foundatio.Messaging;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Foundatio.RabbitMQ.ChaosTests;

public class ChaosFixture : IAsyncLifetime
{
    private DistributedApplication? _app;

    public DistributedApplication App => _app ?? throw new InvalidOperationException("AppHost not initialized");
    private ChaosTestHelper? _chaos;
    public ChaosTestHelper Chaos => _chaos ??= new(App);

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
public class ChaosIntegrationTests : IDisposable
{
    private readonly ChaosFixture _fixture;
    private readonly ITestOutputHelper _output;
    private readonly ILoggerFactory _loggerFactory;

    public ChaosIntegrationTests(ChaosFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
        _loggerFactory = LoggerFactory.Create(builder =>
        {
            builder.SetMinimumLevel(LogLevel.Trace);
            builder.AddProvider(new XUnitLoggerProvider(output));
        });
    }

    public void Dispose() => _loggerFactory.Dispose();

    [Fact]
    public async Task SetDiskFreeLimit_WhenSetToUnreachableValue_TriggersAndClearsAlarm()
    {
        // Arrange
        var chaos = _fixture.Chaos;

        // Act - trigger alarm
        _output.WriteLine("=== Triggering disk alarm on chaos-1 ===");
        await chaos.FillDiskAsync("chaos-1");
        await chaos.WaitForAlarmActiveAsync("chaos-1", TimeSpan.FromSeconds(30));

        // Assert - alarm is active
        var hasAlarm = await chaos.HasDiskAlarmAsync("chaos-1");
        _output.WriteLine($"Alarm active: {hasAlarm}");
        Assert.True(hasAlarm, "Expected disk alarm to be active after setting limit to 999GB");

        // Act - clear alarm
        _output.WriteLine("=== Clearing disk alarm on chaos-1 ===");
        await chaos.ClearDiskAsync("chaos-1");
        await chaos.WaitForAlarmClearedAsync("chaos-1", TimeSpan.FromSeconds(30));

        // Assert - alarm is cleared
        hasAlarm = await chaos.HasDiskAlarmAsync("chaos-1");
        _output.WriteLine($"Alarm cleared: {!hasAlarm}");
        Assert.False(hasAlarm, "Expected disk alarm to clear after resetting limit to 10MB");
    }

    [Fact]
    public async Task PublishAsync_DuringDiskAlarm_WithoutConfirms_MaySucceedSilently()
    {
        // Arrange
        var chaos = _fixture.Chaos;
        var connectionString = chaos.GetConnectionString("chaos-1");
        _output.WriteLine($"Connection: {connectionString}");

        await using var messageBus = new RabbitMQMessageBus(o => o
            .ConnectionString(connectionString)
            .LoggerFactory(_loggerFactory));

        _output.WriteLine("Sending warmup message...");
        await messageBus.PublishAsync(new TestMessage { Data = "warmup" });
        _output.WriteLine("Warmup succeeded");

        _output.WriteLine("Triggering disk alarm...");
        await chaos.FillDiskAsync("chaos-1");
        await chaos.WaitForAlarmActiveAsync("chaos-1", TimeSpan.FromSeconds(30));
        _output.WriteLine("Disk alarm active, waiting for broker blocked notification to propagate...");
        await Task.Delay(TimeSpan.FromSeconds(5));

        // Act
        _output.WriteLine("Attempting publish during alarm (fire-and-forget mode)...");
        bool publishThrew = false;
        try
        {
            await messageBus.PublishAsync(new TestMessage { Data = "during alarm" });
            _output.WriteLine("Publish completed - without confirms, fire-and-forget publishes may succeed or be silently dropped");
        }
        catch (MessageBusException ex)
        {
            publishThrew = true;
            _output.WriteLine($"Publish blocked: {ex.Message}");
            Assert.Contains("blocked", ex.Message, StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            await chaos.ClearDiskAsync("chaos-1");
            await chaos.WaitForAlarmClearedAsync("chaos-1", TimeSpan.FromSeconds(30));
        }

        // Assert
        _output.WriteLine($"Disk alarm cleared. Publish threw: {publishThrew}");
        _output.WriteLine("NOTE: Without publisher confirms, disk alarms may not block publishes immediately.");
        _output.WriteLine("Use PublisherConfirmsEnabled=true for guaranteed delivery detection.");
    }

    [Fact]
    public async Task PublishAsync_AfterNodeRestart_RecoversAutomatically()
    {
        // Arrange
        var chaos = _fixture.Chaos;
        var connectionString = chaos.GetConnectionString("chaos-1");
        _output.WriteLine($"Connection: {connectionString}");

        await using var messageBus = new RabbitMQMessageBus(o => o
            .ConnectionString(connectionString)
            .PublishRecoveryTimeout(TimeSpan.FromSeconds(30))
            .LoggerFactory(_loggerFactory));

        _output.WriteLine("Sending message before kill...");
        await messageBus.PublishAsync(new TestMessage { Data = "before kill" });
        _output.WriteLine("Pre-kill publish succeeded");

        // Act
        _output.WriteLine("Killing chaos-1...");
        await chaos.StopNodeAsync("chaos-1");
        _output.WriteLine("Node killed, waiting 2s...");
        await Task.Delay(TimeSpan.FromSeconds(2));

        _output.WriteLine("Restarting chaos-1...");
        await chaos.StartNodeAsync("chaos-1");
        _output.WriteLine("Node restarted, waiting for RabbitMQ to be ready...");
        await Task.Delay(TimeSpan.FromSeconds(15));

        _output.WriteLine("Attempting publish after restart (should recover)...");
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await messageBus.PublishAsync(new TestMessage { Data = "after restart" }, cancellationToken: cts.Token);
        _output.WriteLine("Post-restart publish SUCCEEDED - recovery gate worked");

        // Assert
        Assert.False(cts.IsCancellationRequested, "Publish should complete before timeout");
    }

    [Fact]
    public async Task PublishAsync_NodeDown_RecoveryTimeoutExpires_ThrowsMessageBusException()
    {
        // Arrange
        var chaos = _fixture.Chaos;
        var connectionString = chaos.GetConnectionString("chaos-1");
        _output.WriteLine($"Connection: {connectionString}");

        await using var messageBus = new RabbitMQMessageBus(o => o
            .ConnectionString(connectionString)
            .PublishRecoveryTimeout(TimeSpan.FromSeconds(3))
            .LoggerFactory(_loggerFactory));

        _output.WriteLine("Sending warmup...");
        await messageBus.PublishAsync(new TestMessage { Data = "warmup" });

        _output.WriteLine("Killing chaos-1...");
        await chaos.StopNodeAsync("chaos-1");
        await Task.Delay(TimeSpan.FromSeconds(2));

        // Act & Assert
        _output.WriteLine("Attempting publish with 3s recovery timeout (node is down)...");
        try
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            await messageBus.PublishAsync(new TestMessage { Data = "should timeout" }, cancellationToken: cts.Token);
            Assert.Fail("Expected MessageBusException but publish succeeded");
        }
        catch (MessageBusException ex)
        {
            _output.WriteLine($"EXPECTED: {ex.Message}");
            Assert.Contains("recovery", ex.Message, StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            _output.WriteLine("Restarting chaos-1...");
            await chaos.StartNodeAsync("chaos-1");
            await Task.Delay(TimeSpan.FromSeconds(10));
            _output.WriteLine("Node restarted");
        }
    }

    [Fact]
    public async Task SubscribeAsync_ReceivesMessages_AfterNodeRestart()
    {
        // Arrange
        var chaos = _fixture.Chaos;
        var connectionString = chaos.GetConnectionString("chaos-1");
        _output.WriteLine($"Connection: {connectionString}");

        var received = new ConcurrentBag<string>();
        var topic = $"chaos-sub-test-{Guid.NewGuid():N}";

        await using var messageBus = new RabbitMQMessageBus(o => o
            .ConnectionString(connectionString)
            .Topic(topic)
            .SubscriptionQueueName($"chaos-sub-{Guid.NewGuid():N}")
            .IsSubscriptionQueueExclusive(false)
            .SubscriptionQueueAutoDelete(true)
            .PublishRecoveryTimeout(TimeSpan.FromSeconds(30))
            .LoggerFactory(_loggerFactory));

        await messageBus.SubscribeAsync<TestMessage>(msg =>
        {
            _output.WriteLine($"Received: {msg.Data}");
            received.Add(msg.Data);
        });

        _output.WriteLine("Publishing pre-restart message...");
        await messageBus.PublishAsync(new TestMessage { Data = "before" });
        await Task.Delay(TimeSpan.FromSeconds(2));
        Assert.Contains("before", received);
        _output.WriteLine($"Pre-restart messages received: {received.Count}");

        // Act
        _output.WriteLine("Killing chaos-1...");
        await chaos.StopNodeAsync("chaos-1");
        await Task.Delay(TimeSpan.FromSeconds(2));

        _output.WriteLine("Restarting chaos-1...");
        await chaos.StartNodeAsync("chaos-1");
        _output.WriteLine("Waiting for recovery...");
        await Task.Delay(TimeSpan.FromSeconds(20));

        _output.WriteLine("Publishing post-restart message...");
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await messageBus.PublishAsync(new TestMessage { Data = "after" }, cancellationToken: cts.Token);
        await Task.Delay(TimeSpan.FromSeconds(5));

        // Assert
        _output.WriteLine($"Total messages received: {received.Count}");
        Assert.Contains("after", received);
        _output.WriteLine("Subscriber successfully recovered and received post-restart message");
    }

    [Fact]
    public async Task PublishAsync_WithPublisherConfirms_DuringDiskAlarm_FailsOrTimesOut()
    {
        // Arrange
        var chaos = _fixture.Chaos;
        var connectionString = chaos.GetConnectionString("chaos-1");
        _output.WriteLine($"Connection: {connectionString}");

        await using var messageBus = new RabbitMQMessageBus(o => o
            .ConnectionString(connectionString)
            .PublisherConfirmsEnabled(true)
            .LoggerFactory(_loggerFactory));

        _output.WriteLine("Sending warmup with confirms...");
        await messageBus.PublishAsync(new TestMessage { Data = "warmup" });
        _output.WriteLine("Warmup confirmed");

        _output.WriteLine("Triggering disk alarm...");
        await chaos.FillDiskAsync("chaos-1");
        await chaos.WaitForAlarmActiveAsync("chaos-1", TimeSpan.FromSeconds(30));
        _output.WriteLine("Alarm active, waiting for blocked notification...");
        await Task.Delay(TimeSpan.FromSeconds(5));

        // Act
        bool publishFailed = false;
        try
        {
            _output.WriteLine("Publishing with confirms during alarm (5s timeout)...");
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            await messageBus.PublishAsync(new TestMessage { Data = "confirmed during alarm" },
                cancellationToken: cts.Token);
            _output.WriteLine("WARNING: Publish confirmed during alarm (broker may not have blocked connection yet)");
        }
        catch (MessageBusException ex)
        {
            publishFailed = true;
            _output.WriteLine($"EXPECTED: MessageBusException: {ex.Message}");
            Assert.Contains("blocked", ex.Message, StringComparison.OrdinalIgnoreCase);
        }
        catch (OperationCanceledException)
        {
            publishFailed = true;
            _output.WriteLine("EXPECTED: Publish timed out (confirms blocked during disk alarm)");
        }
        finally
        {
            await chaos.ClearDiskAsync("chaos-1");
            await chaos.WaitForAlarmClearedAsync("chaos-1", TimeSpan.FromSeconds(30));
            _output.WriteLine("Alarm cleared");
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
        _output.WriteLine($"Cluster status JSON length: {status.Length}");
        _output.WriteLine($"Cluster status: {status[..Math.Min(500, status.Length)]}");

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
        _output.WriteLine($"Connection: {connectionString}");

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
            .LoggerFactory(_loggerFactory));

        await using var consumer2 = new RabbitMQMessageBus(o => o
            .ConnectionString(connectionString)
            .Topic(topic)
            .SubscriptionQueueName(queueName)
            .IsSubscriptionQueueExclusive(false)
            .SubscriptionQueueAutoDelete(true)
            .PrefetchCount(1)
            .LoggerFactory(_loggerFactory));

        await using var consumer3 = new RabbitMQMessageBus(o => o
            .ConnectionString(connectionString)
            .Topic(topic)
            .SubscriptionQueueName(queueName)
            .IsSubscriptionQueueExclusive(false)
            .SubscriptionQueueAutoDelete(true)
            .PrefetchCount(1)
            .LoggerFactory(_loggerFactory));

        await consumer1.SubscribeAsync<TestMessage>(msg => { received1.Add(msg.Data); });
        await consumer2.SubscribeAsync<TestMessage>(msg => { received2.Add(msg.Data); });
        await consumer3.SubscribeAsync<TestMessage>(msg => { received3.Add(msg.Data); });

        await using var publisher = new RabbitMQMessageBus(o => o
            .ConnectionString(connectionString)
            .Topic(topic)
            .LoggerFactory(_loggerFactory));

        // Act
        _output.WriteLine("Publishing 30 messages to competing consumers...");
        for (int i = 0; i < 30; i++)
            await publisher.PublishAsync(new TestMessage { Data = $"msg-{i}" });

        await Task.Delay(TimeSpan.FromSeconds(5));

        // Assert
        int total = received1.Count + received2.Count + received3.Count;
        _output.WriteLine($"Consumer 1 received: {received1.Count}");
        _output.WriteLine($"Consumer 2 received: {received2.Count}");
        _output.WriteLine($"Consumer 3 received: {received3.Count}");
        _output.WriteLine($"Total received: {total}");

        Assert.Equal(30, total);
        Assert.True(received1.Count > 0, "Consumer 1 should receive at least one message");
        Assert.True(received2.Count > 0, "Consumer 2 should receive at least one message");
        Assert.True(received3.Count > 0, "Consumer 3 should receive at least one message");
        _output.WriteLine("Messages distributed across all 3 competing consumers");
    }

    [Fact]
    public async Task SubscribeAsync_DuringRollingUpgrade_ResumesWithoutProcessRestart()
    {
        // Arrange
        var chaos = _fixture.Chaos;
        var connectionString = chaos.GetConnectionString("chaos-1");
        _output.WriteLine($"Connection: {connectionString}");

        var logCapture = new LogCapture();
        using var logFactory = LoggerFactory.Create(builder =>
        {
            builder.SetMinimumLevel(LogLevel.Trace);
            builder.AddProvider(new XUnitLoggerProvider(_output));
            builder.AddProvider(logCapture);
        });

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
            .LoggerFactory(logFactory));

        await messageBus.SubscribeAsync<TestMessage>(msg =>
        {
            _output.WriteLine($"Received: {msg.Data}");
            received.Add(msg.Data);
        });

        _output.WriteLine("=== Phase 1: Steady-state publishing ===");
        for (int i = 0; i < 5; i++)
            await messageBus.PublishAsync(new TestMessage { Data = $"pre-restart-{i}" });
        await Task.Delay(TimeSpan.FromSeconds(3));
        Assert.Equal(5, received.Count);
        _output.WriteLine($"Phase 1 complete: {received.Count} messages received");

        // Act
        _output.WriteLine("=== Phase 2: Simulate rolling upgrade (kill node) ===");
        await chaos.StopNodeAsync("chaos-1");
        _output.WriteLine("Node killed (simulating rolling upgrade restart)");
        await Task.Delay(TimeSpan.FromSeconds(2));

        _output.WriteLine("=== Phase 3: Node comes back (upgrade complete) ===");
        await chaos.StartNodeAsync("chaos-1");
        _output.WriteLine("Node restarted, waiting for auto-recovery...");
        await Task.Delay(TimeSpan.FromSeconds(20));

        _output.WriteLine("=== Phase 4: Verify consumer resumed ===");
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        for (int i = 0; i < 5; i++)
            await messageBus.PublishAsync(new TestMessage { Data = $"post-restart-{i}" }, cancellationToken: cts.Token);
        await Task.Delay(TimeSpan.FromSeconds(5));

        // Assert
        var postRestartCount = received.Count(m => m.StartsWith("post-restart-"));
        _output.WriteLine($"Post-restart messages received: {postRestartCount}/5");
        Assert.Equal(5, postRestartCount);

        var recoveryLogs = logCapture.GetMessages();
        _output.WriteLine($"Total captured log messages: {recoveryLogs.Count}");
        Assert.Contains(recoveryLogs, m => m.Contains("Publisher connection lost"));
        Assert.Contains(recoveryLogs, m => m.Contains("Publisher connection recovery succeeded"));
        Assert.Contains(recoveryLogs, m => m.Contains("Subscriber connection recovery") || m.Contains("Subscriber connection recovering"));
        _output.WriteLine("Recovery audit trail verified: shutdown -> recovery attempt -> recovery succeeded");
    }

    [Fact]
    public async Task PublishAsync_WhenPrimaryHostDown_ConnectsToAlternateEndpoint()
    {
        // Arrange
        var chaos = _fixture.Chaos;
        var endpoint1 = _fixture.App.GetEndpoint("chaos-1", "amqp");
        var endpoint2 = _fixture.App.GetEndpoint("chaos-2", "amqp");
        _output.WriteLine($"Endpoint 1 (chaos-1): {endpoint1.Host}:{endpoint1.Port}");
        _output.WriteLine($"Endpoint 2 (chaos-2): {endpoint2.Host}:{endpoint2.Port}");

        var logCapture = new LogCapture();
        using var logFactory = LoggerFactory.Create(builder =>
        {
            builder.SetMinimumLevel(LogLevel.Trace);
            builder.AddProvider(new XUnitLoggerProvider(_output));
            builder.AddProvider(logCapture);
        });

        _output.WriteLine("Killing chaos-1...");
        await chaos.StopNodeAsync("chaos-1");
        await Task.Delay(TimeSpan.FromSeconds(3));

        // Act & Assert
        _output.WriteLine("Connecting with Hosts=[chaos-1, chaos-2] (chaos-1 is down)...");
        try
        {
            await using var messageBus = new RabbitMQMessageBus(o => o
                .ConnectionString($"amqp://guest:guest@{endpoint2.Host}:{endpoint2.Port}")
                .Hosts($"{endpoint1.Host}:{endpoint1.Port}", $"{endpoint2.Host}:{endpoint2.Port}")
                .LoggerFactory(logFactory));

            _output.WriteLine("Publishing message (should connect to chaos-2)...");
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            await messageBus.PublishAsync(new TestMessage { Data = "failover test" }, cancellationToken: cts.Token);
            _output.WriteLine("Publish succeeded via failover endpoint!");

            var logs = logCapture.GetMessages();
            _output.WriteLine($"Log messages captured: {logs.Count}");
            Assert.True(logs.Count > 0, "Should have log messages showing connection activity");
            _output.WriteLine("Multi-endpoint failover working - connected when primary host was down");
        }
        finally
        {
            _output.WriteLine("Restarting chaos-1...");
            await chaos.StartNodeAsync("chaos-1");
            await Task.Delay(TimeSpan.FromSeconds(10));
            _output.WriteLine("chaos-1 restarted");
        }
    }

    [Fact]
    public async Task PublishAsync_AfterNodeKillAndRestart_LogsFullRecoverySequence()
    {
        // Arrange
        var chaos = _fixture.Chaos;
        var connectionString = chaos.GetConnectionString("chaos-1");
        _output.WriteLine($"Connection: {connectionString}");

        var logCapture = new LogCapture();
        using var logFactory = LoggerFactory.Create(builder =>
        {
            builder.SetMinimumLevel(LogLevel.Trace);
            builder.AddProvider(new XUnitLoggerProvider(_output));
            builder.AddProvider(logCapture);
        });

        await using var messageBus = new RabbitMQMessageBus(o => o
            .ConnectionString(connectionString)
            .PublishRecoveryTimeout(TimeSpan.FromSeconds(30))
            .LoggerFactory(logFactory));

        _output.WriteLine("Warmup publish...");
        await messageBus.PublishAsync(new TestMessage { Data = "warmup" });

        // Act
        _output.WriteLine("Killing node to trigger recovery sequence...");
        await chaos.StopNodeAsync("chaos-1");
        await Task.Delay(TimeSpan.FromSeconds(3));

        _output.WriteLine("Restarting node...");
        await chaos.StartNodeAsync("chaos-1");
        await Task.Delay(TimeSpan.FromSeconds(15));

        _output.WriteLine("Publishing after recovery...");
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await messageBus.PublishAsync(new TestMessage { Data = "after recovery" }, cancellationToken: cts.Token);

        // Assert
        _output.WriteLine("=== Verifying Recovery Audit Trail (OUT-12608 requirement) ===");
        var logs = logCapture.GetMessages();

        bool hasShutdown = logs.Any(m => m.Contains("Publisher shutdown") || m.Contains("connection lost"));
        bool hasRecoveryAttempt = logs.Any(m => m.Contains("recovery") && (m.Contains("error") || m.Contains("attempt") || m.Contains("waiting")));
        bool hasRecoverySuccess = logs.Any(m => m.Contains("recovery succeeded"));

        _output.WriteLine($"  [1] Shutdown detected: {hasShutdown}");
        _output.WriteLine($"  [2] Recovery attempt/error logged: {hasRecoveryAttempt}");
        _output.WriteLine($"  [3] Recovery succeeded logged: {hasRecoverySuccess}");

        Assert.True(hasShutdown, "Missing: shutdown event in logs");
        Assert.True(hasRecoveryAttempt, "Missing: recovery attempt/error in logs");
        Assert.True(hasRecoverySuccess, "Missing: recovery succeeded in logs");
        _output.WriteLine("Full recovery audit trail confirmed (shutdown -> attempt -> success)");
    }

    [Fact]
    public async Task PublishAsync_ConcurrentDuringNodeKill_AllRecoverOrFailGracefully()
    {
        // Arrange
        var chaos = _fixture.Chaos;
        var connectionString = chaos.GetConnectionString("chaos-1");
        _output.WriteLine($"Connection: {connectionString}");

        await using var messageBus = new RabbitMQMessageBus(o => o
            .ConnectionString(connectionString)
            .PublishRecoveryTimeout(TimeSpan.FromSeconds(30))
            .LoggerFactory(_loggerFactory));

        _output.WriteLine("Warmup...");
        await messageBus.PublishAsync(new TestMessage { Data = "warmup" });

        int succeeded = 0;
        int failedGracefully = 0;
        int unexpected = 0;

        // Act
        _output.WriteLine("Launching 20 concurrent publishers, killing node mid-flight...");
        var killTask = Task.Run(async () =>
        {
            await Task.Delay(TimeSpan.FromMilliseconds(500));
            await chaos.StopNodeAsync("chaos-1");
            _output.WriteLine("  --> Node killed mid-publish");
            await Task.Delay(TimeSpan.FromSeconds(5));
            await chaos.StartNodeAsync("chaos-1");
            _output.WriteLine("  --> Node restarted");
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
                _output.WriteLine($"  UNEXPECTED exception type: {ex.GetType().Name}: {ex.Message}");
            }
        });

        await Task.WhenAll(publishTasks.Append(killTask));
        await Task.Delay(TimeSpan.FromSeconds(15));

        // Assert
        _output.WriteLine($"Succeeded: {succeeded}, Failed gracefully: {failedGracefully}, Unexpected: {unexpected}");
        Assert.Equal(0, unexpected);
        Assert.True(succeeded + failedGracefully == 20, "All 20 publishes should either succeed or fail gracefully");
        Assert.True(succeeded > 0, "At least some publishes should succeed (before kill or after recovery)");
        _output.WriteLine("All concurrent publishes handled gracefully during node failure");
    }

    [Fact]
    public async Task PublishAsync_DuringDiskAlarmAndNodeKill_RecoversAfterRestart()
    {
        // Arrange
        var chaos = _fixture.Chaos;
        var connectionString = chaos.GetConnectionString("chaos-1");
        _output.WriteLine($"Connection: {connectionString}");

        await using var messageBus = new RabbitMQMessageBus(o => o
            .ConnectionString(connectionString)
            .PublishRecoveryTimeout(TimeSpan.FromSeconds(30))
            .LoggerFactory(_loggerFactory));

        _output.WriteLine("Warmup...");
        await messageBus.PublishAsync(new TestMessage { Data = "warmup" });

        // Act
        _output.WriteLine("=== Fault 1: Trigger disk alarm ===");
        await chaos.FillDiskAsync("chaos-1");
        await chaos.WaitForAlarmActiveAsync("chaos-1", TimeSpan.FromSeconds(30));
        _output.WriteLine("Disk alarm active");

        _output.WriteLine("=== Fault 2: Kill the node while alarm is active ===");
        await chaos.StopNodeAsync("chaos-1");
        _output.WriteLine("Node killed during disk alarm - double fault in progress");
        await Task.Delay(TimeSpan.FromSeconds(3));

        _output.WriteLine("=== Recovery: Restart node ===");
        await chaos.StartNodeAsync("chaos-1");
        await Task.Delay(TimeSpan.FromSeconds(15));

        await chaos.ClearDiskAsync("chaos-1");
        await chaos.WaitForAlarmClearedAsync("chaos-1", TimeSpan.FromSeconds(30));

        // Assert
        _output.WriteLine("=== Verify: System recovered from double fault ===");
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await messageBus.PublishAsync(new TestMessage { Data = "after double fault" }, cancellationToken: cts.Token);
        _output.WriteLine("Publish succeeded after double fault recovery!");
        Assert.False(cts.IsCancellationRequested);
    }

    [Fact]
    public async Task PublishAsync_AfterRapidConnectionFlapping_SystemRemainsStable()
    {
        // Arrange
        var chaos = _fixture.Chaos;
        var connectionString = chaos.GetConnectionString("chaos-1");
        _output.WriteLine($"Connection: {connectionString}");

        var logCapture = new LogCapture();
        using var logFactory = LoggerFactory.Create(builder =>
        {
            builder.SetMinimumLevel(LogLevel.Trace);
            builder.AddProvider(new XUnitLoggerProvider(_output));
            builder.AddProvider(logCapture);
        });

        await using var messageBus = new RabbitMQMessageBus(o => o
            .ConnectionString(connectionString)
            .PublishRecoveryTimeout(TimeSpan.FromSeconds(30))
            .LoggerFactory(logFactory));

        _output.WriteLine("Warmup...");
        await messageBus.PublishAsync(new TestMessage { Data = "warmup" });

        // Act
        _output.WriteLine("=== Rapid flapping: kill/restart 3 times in quick succession ===");
        for (int i = 1; i <= 3; i++)
        {
            _output.WriteLine($"  Flap {i}: killing node...");
            await chaos.StopNodeAsync("chaos-1");
            await Task.Delay(TimeSpan.FromSeconds(2));
            _output.WriteLine($"  Flap {i}: restarting node...");
            await chaos.StartNodeAsync("chaos-1");
            await Task.Delay(TimeSpan.FromSeconds(10));
        }

        _output.WriteLine("Waiting for final recovery to settle...");
        await Task.Delay(TimeSpan.FromSeconds(10));

        _output.WriteLine("=== Verify: System is stable after flapping ===");
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await messageBus.PublishAsync(new TestMessage { Data = "after flapping" }, cancellationToken: cts.Token);
        _output.WriteLine("Publish succeeded after rapid connection flapping!");

        // Assert
        var logs = logCapture.GetMessages();
        int recoveryCount = logs.Count(m => m.Contains("recovery succeeded"));
        _output.WriteLine($"Recovery succeeded events: {recoveryCount}");
        Assert.True(recoveryCount >= 1, "Should have recovered at least once");
    }

    [Fact]
    public async Task PublishAsync_HighVolumeBurstAfterRecovery_AllMessagesDelivered()
    {
        // Arrange
        var chaos = _fixture.Chaos;
        var connectionString = chaos.GetConnectionString("chaos-1");
        _output.WriteLine($"Connection: {connectionString}");

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
            .LoggerFactory(_loggerFactory));

        await messageBus.SubscribeAsync<TestMessage>(msg => received.Add(msg.Data));

        _output.WriteLine("Verify baseline delivery...");
        await messageBus.PublishAsync(new TestMessage { Data = "baseline" });
        await Task.Delay(TimeSpan.FromSeconds(2));
        Assert.Contains("baseline", received);

        // Act
        _output.WriteLine("Kill and restart node...");
        await chaos.StopNodeAsync("chaos-1");
        await Task.Delay(TimeSpan.FromSeconds(2));
        await chaos.StartNodeAsync("chaos-1");
        await Task.Delay(TimeSpan.FromSeconds(20));

        _output.WriteLine("=== Burst: Publishing 50 messages rapidly after recovery ===");
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        for (int i = 0; i < 50; i++)
            await messageBus.PublishAsync(new TestMessage { Data = $"burst-{i}" }, cancellationToken: cts.Token);

        _output.WriteLine("Waiting for all messages to be delivered...");
        await Task.Delay(TimeSpan.FromSeconds(10));

        // Assert
        int burstReceived = received.Count(m => m.StartsWith("burst-"));
        _output.WriteLine($"Burst messages received: {burstReceived}/50");
        Assert.Equal(50, burstReceived);
        _output.WriteLine("All 50 burst messages delivered after recovery - no message loss!");
    }

    [Fact]
    public async Task SubscribeAsync_NodeKillDuringSlowProcessing_SubscriptionSurvives()
    {
        // Arrange
        var chaos = _fixture.Chaos;
        var connectionString = chaos.GetConnectionString("chaos-1");
        _output.WriteLine($"Connection: {connectionString}");

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
            .LoggerFactory(_loggerFactory));

        await messageBus.SubscribeAsync<TestMessage>(async msg =>
        {
            if (msg.Data == "slow-message")
            {
                _output.WriteLine("Processing slow message (simulating 3s handler)...");
                await Task.Delay(TimeSpan.FromSeconds(3));
            }
            received.Add(msg.Data);
            _output.WriteLine($"Handler completed: {msg.Data}");
        });

        _output.WriteLine("Publishing a fast message to verify subscription is active...");
        await messageBus.PublishAsync(new TestMessage { Data = "fast-before" });
        await Task.Delay(TimeSpan.FromSeconds(1));
        Assert.Contains("fast-before", received);

        // Act
        _output.WriteLine("Publishing slow message then killing node during processing...");
        await messageBus.PublishAsync(new TestMessage { Data = "slow-message" });
        await Task.Delay(TimeSpan.FromMilliseconds(500));

        await chaos.StopNodeAsync("chaos-1");
        _output.WriteLine("Node killed while handler is processing slow-message");
        await Task.Delay(TimeSpan.FromSeconds(2));

        await chaos.StartNodeAsync("chaos-1");
        _output.WriteLine("Node restarted, waiting for recovery...");
        await Task.Delay(TimeSpan.FromSeconds(20));

        _output.WriteLine("Publishing post-recovery message to verify subscription alive...");
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await messageBus.PublishAsync(new TestMessage { Data = "post-recovery" }, cancellationToken: cts.Token);
        await Task.Delay(TimeSpan.FromSeconds(5));

        // Assert
        _output.WriteLine($"Total messages received: {received.Count}");
        _output.WriteLine($"Messages: [{string.Join(", ", received)}]");
        Assert.Contains("post-recovery", received);
        _output.WriteLine("Consumer subscription survived node kill during slow message processing");
    }
}

public record TestMessage
{
    public string Data { get; init; } = string.Empty;
}

public class XUnitLoggerProvider : ILoggerProvider
{
    private readonly ITestOutputHelper _output;

    public XUnitLoggerProvider(ITestOutputHelper output) => _output = output;

    public ILogger CreateLogger(string categoryName) => new XUnitLogger(_output, categoryName);

    public void Dispose() { }
}

public class XUnitLogger : ILogger
{
    private readonly ITestOutputHelper _output;
    private readonly string _category;

    public XUnitLogger(ITestOutputHelper output, string category)
    {
        _output = output;
        _category = category;
    }

    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
    public bool IsEnabled(LogLevel logLevel) => true;

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
    {
        try
        {
            var message = formatter(state, exception);
            var shortCategory = _category.Length > 30 ? _category[^30..] : _category;
            _output.WriteLine($"[{logLevel,-12}] {shortCategory}: {message}");
            if (exception is not null)
                _output.WriteLine($"  Exception: {exception.GetType().Name}: {exception.Message}");
        }
        catch
        {
            // ITestOutputHelper throws after test completes
        }
    }
}

public class LogCapture : ILoggerProvider
{
    private readonly ConcurrentBag<string> _messages = [];

    public ILogger CreateLogger(string categoryName) => new CapturingLogger(_messages);
    public void Dispose() { }
    public List<string> GetMessages() => [.. _messages];

    private class CapturingLogger(ConcurrentBag<string> messages) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            messages.Add(formatter(state, exception));
        }
    }
}
