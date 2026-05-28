using System;
using System.Threading;
using System.Threading.Tasks;
using Aspire.Hosting;
using Aspire.Hosting.ApplicationModel;
using Aspire.Hosting.Testing;
using Xunit;

namespace Foundatio.RabbitMQ.Tests;

public class AspireFixture : IAsyncLifetime
{
    private static readonly Lazy<Task<DistributedApplication?>> SharedApp = new(StartAppAsync, LazyThreadSafetyMode.ExecutionAndPublication);

    private DistributedApplication? _app;
    public DistributedApplication App => _app ?? throw new InvalidOperationException("Fixture not initialized - Aspire AppHost failed to start");
    public string? MessagingConnectionString { get; private set; }
    public string? MessagingDelayedConnectionString { get; private set; }
    public bool ChaosClusterAvailable { get; private set; }
    public bool IsAvailable => _app is not null && MessagingConnectionString is not null;

    public async ValueTask InitializeAsync()
    {
        _app = await SharedApp.Value;
        if (_app is null)
            return;

        MessagingConnectionString = await _app.GetConnectionStringAsync("messaging");
        if (MessagingConnectionString is null)
            return;

        try
        {
            await _app.ResourceNotifications.WaitForResourceAsync(
                "messaging-delayed", KnownResourceStates.Running)
                .WaitAsync(TimeSpan.FromSeconds(60));

            var delayedEndpoint = _app.GetEndpoint("messaging-delayed", "amqp");
            MessagingDelayedConnectionString = $"amqp://guest:guest@{delayedEndpoint.Host}:{delayedEndpoint.Port}";
        }
        catch (Exception)
        {
            MessagingDelayedConnectionString = null;
        }

        try
        {
            for (int i = 1; i <= 3; i++)
            {
                await _app.ResourceNotifications.WaitForResourceHealthyAsync($"chaos-{i}")
                    .WaitAsync(TimeSpan.FromSeconds(30));
            }
            ChaosClusterAvailable = true;
        }
        catch (Exception)
        {
            ChaosClusterAvailable = false;
        }
    }

    private static async Task<DistributedApplication?> StartAppAsync()
    {
        try
        {
            var appHost = await DistributedApplicationTestingBuilder
                .CreateAsync<Projects.Foundatio_RabbitMQ_AppHost>()
                .WaitAsync(TimeSpan.FromMinutes(2));

            var app = await appHost.BuildAsync()
                .WaitAsync(TimeSpan.FromMinutes(1));

            using var startCts = new CancellationTokenSource(TimeSpan.FromMinutes(2));
            await app.StartAsync(startCts.Token);

            await app.ResourceNotifications.WaitForResourceHealthyAsync("messaging")
                .WaitAsync(TimeSpan.FromSeconds(60));

            return app;
        }
        catch (Exception)
        {
            return null;
        }
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
