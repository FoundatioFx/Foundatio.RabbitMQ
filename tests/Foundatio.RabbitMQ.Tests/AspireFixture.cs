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
    private static readonly Lazy<Task<DistributedApplication>> s_sharedApp = new(StartAppAsync, LazyThreadSafetyMode.ExecutionAndPublication);

    public DistributedApplication App => s_sharedApp.Value.GetAwaiter().GetResult();
    public string MessagingConnectionString { get; private set; } = string.Empty;
    public string MessagingDelayedConnectionString { get; private set; } = string.Empty;

    public async ValueTask InitializeAsync()
    {
        var app = await s_sharedApp.Value;

        var connectionString = await app.GetConnectionStringAsync("messaging");
        MessagingConnectionString = connectionString ?? throw new InvalidOperationException("Could not get messaging connection string");

        try
        {
            await app.ResourceNotifications.WaitForResourceAsync(
                "messaging-delayed", KnownResourceStates.Running)
                .WaitAsync(TimeSpan.FromSeconds(120));

            var delayedEndpoint = app.GetEndpoint("messaging-delayed", "amqp");
            MessagingDelayedConnectionString = $"amqp://guest:guest@{delayedEndpoint.Host}:{delayedEndpoint.Port}";
        }
        catch (TimeoutException)
        {
            MessagingDelayedConnectionString = string.Empty;
        }
    }

    private static async Task<DistributedApplication> StartAppAsync()
    {
        var appHost = await DistributedApplicationTestingBuilder
            .CreateAsync<Projects.Foundatio_RabbitMQ_AppHost>();

        var app = await appHost.BuildAsync();
        using var startCts = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        await app.StartAsync(startCts.Token);

        await app.ResourceNotifications.WaitForResourceHealthyAsync("messaging")
            .WaitAsync(TimeSpan.FromSeconds(120));

        return app;
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
