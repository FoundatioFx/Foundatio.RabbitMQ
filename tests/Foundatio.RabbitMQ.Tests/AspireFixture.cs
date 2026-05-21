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
    private DistributedApplication? _app;

    public DistributedApplication App => _app ?? throw new InvalidOperationException("AppHost not initialized");
    public string MessagingConnectionString { get; private set; } = string.Empty;
    public string MessagingDelayedConnectionString { get; private set; } = string.Empty;

    public async ValueTask InitializeAsync()
    {
        var appHost = await DistributedApplicationTestingBuilder
            .CreateAsync<Projects.Foundatio_RabbitMQ_AppHost>();

        _app = await appHost.BuildAsync();
        using var startCts = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        await _app.StartAsync(startCts.Token);

        await _app.ResourceNotifications.WaitForResourceHealthyAsync("messaging")
            .WaitAsync(TimeSpan.FromSeconds(120));

        var connectionString = await _app.GetConnectionStringAsync("messaging");
        MessagingConnectionString = connectionString ?? throw new InvalidOperationException("Could not get messaging connection string");

        try
        {
            await _app.ResourceNotifications.WaitForResourceAsync(
                "messaging-delayed", KnownResourceStates.Running)
                .WaitAsync(TimeSpan.FromSeconds(120));

            var delayedEndpoint = _app.GetEndpoint("messaging-delayed", "amqp");
            MessagingDelayedConnectionString = $"amqp://guest:guest@{delayedEndpoint.Host}:{delayedEndpoint.Port}";
        }
        catch (TimeoutException)
        {
            MessagingDelayedConnectionString = string.Empty;
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_app is not null)
        {
            await _app.DisposeAsync();
        }
    }
}

[CollectionDefinition("Aspire")]
public class AspireCollection : ICollectionFixture<AspireFixture>;
