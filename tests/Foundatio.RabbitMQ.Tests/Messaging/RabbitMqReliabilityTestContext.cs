using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Foundatio.Messaging;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

namespace Foundatio.RabbitMQ.Tests.Messaging;

internal sealed class RabbitMqReliabilityTestContext : IAsyncDisposable
{
    private readonly IConnection _connection;
    private readonly string _connectionString;
    private readonly ILoggerFactory _loggerFactory;
    internal IChannel Admin { get; }
    internal string Topic { get; } = $"reliability-{Guid.NewGuid():N}";
    internal string Source => $"{Topic}-source";
    internal string Healthy => $"{Topic}-healthy";
    internal string Dlx => $"{Topic}-dlx";
    internal string Destination => $"{Topic}-destination";

    private RabbitMqReliabilityTestContext(IConnection connection, IChannel admin, string connectionString, ILoggerFactory loggerFactory)
    {
        _connection = connection;
        Admin = admin;
        _connectionString = connectionString;
        _loggerFactory = loggerFactory;
    }

    internal static async Task<RabbitMqReliabilityTestContext> CreateAsync(string connectionString, ILoggerFactory loggerFactory, CancellationToken token)
    {
        var factory = new ConnectionFactory { Uri = new Uri(connectionString), AutomaticRecoveryEnabled = false };
        var connection = await factory.CreateConnectionAsync(token);
        try
        {
            var channel = await connection.CreateChannelAsync(new CreateChannelOptions(true, true), token);
            return new RabbitMqReliabilityTestContext(connection, channel, connectionString, loggerFactory);
        }
        catch
        {
            await connection.DisposeAsync();
            throw;
        }
    }

    internal RabbitMQMessageBusOptions Options(bool quorum = false, bool healthy = false) => new()
    {
        ConnectionString = _connectionString,
        LoggerFactory = _loggerFactory,
        Topic = Topic,
        SubscriptionQueueName = healthy ? Healthy : Source,
        IsDurable = true,
        IsSubscriptionQueueExclusive = false,
        SubscriptionQueueAutoDelete = false,
        AcknowledgementStrategy = AcknowledgementStrategy.Automatic,
        RequireSuccessfulDispatch = true,
        DeadLetterExchange = Dlx,
        DeadLetterRoutingKey = "quarantine",
        PublisherConfirmsEnabled = true,
        PrefetchCount = 1,
        DeliveryLimit = 1,
        ShutdownTimeout = TimeSpan.FromSeconds(2),
        NetworkRecoveryInterval = TimeSpan.FromSeconds(1),
        Arguments = quorum
            ? new Dictionary<string, object?> { ["x-queue-type"] = "quorum", ["x-delivery-limit"] = -1L }
            : new Dictionary<string, object?> { ["x-queue-type"] = "classic" }
    };

    internal async Task CreateDestinationAsync(CancellationToken token, bool bounded = false)
    {
        await Admin.ExchangeDeclareAsync(Dlx, "direct", true, cancellationToken: token);
        var arguments = bounded
            ? new Dictionary<string, object?> { ["x-max-length"] = 1L, ["x-overflow"] = "reject-publish" }
            : null;
        await Admin.QueueDeclareAsync(Destination, true, false, false, arguments: arguments, cancellationToken: token);
        await Admin.QueueBindAsync(Destination, Dlx, "quarantine", cancellationToken: token);
    }

    internal async Task<BasicGetResult> ReadAsync(string queue, CancellationToken token)
    {
        while (true)
        {
            var result = await Admin.BasicGetAsync(queue, false, token);
            if (result is not null)
                return result;
            await Task.Delay(TimeSpan.FromMilliseconds(50), token);
        }
    }

    internal static async Task WaitAsync(Func<bool> condition, CancellationToken token)
    {
        while (!condition())
            await Task.Delay(TimeSpan.FromMilliseconds(50), token);
    }

    public async ValueTask DisposeAsync()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        try
        {
            foreach (string queue in new[] { Source, Healthy, Destination })
            {
                await using var channel = await _connection.CreateChannelAsync(cancellationToken: timeout.Token);
                try
                {
                    await channel.QueueDeleteAsync(queue, cancellationToken: timeout.Token);
                }
                catch (OperationInterruptedException exception) when (exception.ShutdownReason?.ReplyCode == 404)
                {
                    // A setup failure may occur before this test-owned resource exists.
                }
            }
            foreach (string exchange in new[] { Topic, Dlx })
            {
                await using var channel = await _connection.CreateChannelAsync(cancellationToken: timeout.Token);
                try
                {
                    await channel.ExchangeDeleteAsync(exchange, cancellationToken: timeout.Token);
                }
                catch (OperationInterruptedException exception) when (exception.ShutdownReason?.ReplyCode == 404)
                {
                }
            }
        }
        finally
        {
            await Admin.DisposeAsync();
            await _connection.DisposeAsync();
        }
    }
}
