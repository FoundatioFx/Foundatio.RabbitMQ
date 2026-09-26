using System;
using System.IO;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using Aspire.Hosting;
using Aspire.Hosting.ApplicationModel;
using Aspire.Hosting.Testing;
using Xunit;

namespace Foundatio.RabbitMQ.Tests;

public class AspireFixture : IAsyncLifetime
{
    private IDistributedApplicationTestingBuilder? _builder;
    private DistributedApplication? _app;
    private RabbitMqTestCertificates? _certificates;

    public DistributedApplication App => _app ?? throw new InvalidOperationException("RabbitMQ test infrastructure is not initialized.");
    public string? MessagingConnectionString { get; private set; }
    public string? MessagingDelayedConnectionString { get; private set; }
    public string? TlsConnectionString { get; private set; }
    public string? UntrustedTlsConnectionString { get; private set; }
    public bool ChaosClusterAvailable { get; private set; }
    public bool IsAvailable => _app is not null && MessagingConnectionString is not null;

    public async ValueTask InitializeAsync()
    {
        bool required = TestInfrastructurePolicy.Required;
        try
        {
            using var startup = new CancellationTokenSource(TimeSpan.FromMinutes(5));
            _builder = await DistributedApplicationTestingBuilder.CreateAsync<Projects.Foundatio_RabbitMQ_AppHost>(cancellationToken: startup.Token);
            try
            {
                _certificates = await RabbitMqTestCertificates.CreateAsync(startup.Token);
                AddTlsBroker("messaging-tls", _certificates.TrustedDirectory);
                AddTlsBroker("messaging-tls-untrusted", _certificates.UntrustedDirectory);
            }
            catch (CryptographicException exception) when (!required)
            {
                TestInfrastructurePolicy.ReportUnavailable("TLS certificate trust store", exception, required: false);
            }
            _app = await _builder.BuildAsync(startup.Token);
            await _app.StartAsync(startup.Token);
            await Task.WhenAll(
                _app.ResourceNotifications.WaitForResourceHealthyAsync("messaging", startup.Token),
                _app.ResourceNotifications.WaitForResourceAsync("messaging-delayed", KnownResourceStates.Running, startup.Token),
                _app.ResourceNotifications.WaitForResourceHealthyAsync("chaos-1", startup.Token),
                _app.ResourceNotifications.WaitForResourceHealthyAsync("chaos-2", startup.Token),
                _app.ResourceNotifications.WaitForResourceHealthyAsync("chaos-3", startup.Token),
                _certificates is not null ? _app.ResourceNotifications.WaitForResourceHealthyAsync("messaging-tls", startup.Token) : Task.CompletedTask,
                _certificates is not null ? _app.ResourceNotifications.WaitForResourceHealthyAsync("messaging-tls-untrusted", startup.Token) : Task.CompletedTask);

            MessagingConnectionString = await _app.GetConnectionStringAsync("messaging", startup.Token)
                ?? throw new InvalidOperationException("The messaging resource did not provide a connection string.");
            var delayed = _app.GetEndpoint("messaging-delayed", "amqp");
            MessagingDelayedConnectionString = $"amqp://guest:guest@{delayed.Host}:{delayed.Port}";
            if (_certificates is not null)
            {
                TlsConnectionString = GetTlsConnectionString("messaging-tls");
                UntrustedTlsConnectionString = GetTlsConnectionString("messaging-tls-untrusted");
            }
            ChaosClusterAvailable = true;
        }
        catch (Exception exception) when (exception is not OutOfMemoryException and not StackOverflowException)
        {
            await DisposeAsync();
            TestInfrastructurePolicy.ReportUnavailable("Aspire collection startup", exception, required);
        }
    }

    public async ValueTask DisposeAsync()
    {
        var app = _app;
        var builder = _builder;
        var certificates = _certificates;
        _app = null;
        _builder = null;
        _certificates = null;
        MessagingConnectionString = null;
        MessagingDelayedConnectionString = null;
        TlsConnectionString = null;
        UntrustedTlsConnectionString = null;
        ChaosClusterAvailable = false;

        try
        {
            if (app is not null)
                await app.DisposeAsync();
        }
        finally
        {
            try
            {
                if (builder is not null)
                    await builder.DisposeAsync();
            }
            finally
            {
                certificates?.Dispose();
            }
        }
    }

    private void AddTlsBroker(string name, string directory)
    {
        _builder!.AddContainer(name, "rabbitmq", "4.2.5-management")
            .WithEnvironment("RABBITMQ_DEFAULT_USER", "tls-test")
            .WithEnvironment("RABBITMQ_DEFAULT_PASS", _certificates!.Password)
            .WithBindMount(directory, "/certificates", isReadOnly: true)
            .WithBindMount(Path.Combine(directory, "rabbitmq.conf"), "/etc/rabbitmq/rabbitmq.conf", isReadOnly: true)
            .WithEndpoint(targetPort: 5671, name: "amqps", scheme: "tcp")
            .WithEndpoint(targetPort: 15692, name: "prometheus", scheme: "http")
            .WithHttpHealthCheck("/metrics", endpointName: "prometheus");
    }

    private string GetTlsConnectionString(string resource)
    {
        var endpoint = App.GetEndpoint(resource, "amqps");
        return new UriBuilder("amqps", "localhost", endpoint.Port)
        {
            UserName = "tls-test",
            Password = _certificates!.Password
        }.Uri.AbsoluteUri;
    }
}
