using System;
using System.Collections.Generic;
using System.CommandLine;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Foundatio.Messaging;
using Foundatio.RabbitMQ;
using Foundatio.Utility;
using Microsoft.Extensions.Logging;
using OpenTelemetry;
using OpenTelemetry.Logs;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using RabbitMQ.Client;

Option<string> connectionStringOption = new("--connection-string")
{
    Description = "RabbitMQ connection string (provides credentials and vhost)",
    DefaultValueFactory = _ => Environment.GetEnvironmentVariable("ConnectionStrings__messaging") ?? "amqp://localhost:5672"
};

Option<string> hostsOption = new("--hosts")
{
    Description = "Comma-separated list of hosts for failover (e.g., localhost:5672,localhost:5673,localhost:5674)",
    DefaultValueFactory = _ => Environment.GetEnvironmentVariable("RABBITMQ_HOSTS") ?? ""
};

Option<string> topicOption = new("--topic")
{
    Description = "Message topic/exchange name",
    DefaultValueFactory = _ => "sample-topic"
};

Option<bool> durableOption = new("--durable")
{
    Description = "Use durable queues that survive broker restarts"
};

Option<bool> delayedOption = new("--delayed")
{
    Description = "Use delayed exchange (connects to port 5673)"
};

Option<string> acknowledgmentStrategyOption = new("--acknowledgment-strategy")
{
    Description = "Acknowledgment strategy: fireandforget or automatic",
    DefaultValueFactory = _ => "fireandforget"
};

Option<ushort> prefetchCountOption = new("--prefetch-count")
{
    Description = "Consumer prefetch count",
    DefaultValueFactory = _ => 10
};

Option<long> deliveryLimitOption = new("--delivery-limit")
{
    Description = "Maximum failed redeliveries after the initial attempt before terminal handling",
    DefaultValueFactory = _ => 2
};

Option<int> subscriberCountOption = new("--subscriber-count")
{
    Description = "Number of concurrent subscribers",
    DefaultValueFactory = _ => 1
};

Option<string> groupIdOption = new("--group-id")
{
    Description = "Subscriber group identifier for queue naming",
    DefaultValueFactory = _ => "sample-subscriber"
};

Option<LogLevel> logLevelOption = new("--log-level")
{
    Description = "Minimum log level",
    DefaultValueFactory = _ => LogLevel.Information
};

Option<string> queueTypeOption = new("--queue-type")
{
    Description = "Queue type: classic or quorum (does not convert existing queues)",
    DefaultValueFactory = _ => "classic"
};
Option<bool> requiredOption = new("--require-successful-dispatch") { Description = "Require matching handlers and a quarantine destination" };
Option<string> deadLetterOption = new("--dead-letter-exchange") { Description = "Preprovisioned terminal exchange" };
Option<bool> provisionOption = new("--provision-quarantine") { Description = "Create the sample's bounded durable quarantine topology" };
Option<int> failEveryOption = new("--fail-every") { Description = "Permanently fail every Nth sample order; zero disables failures" };
Option<long> maxBytesOption = new("--max-length-bytes")
{
    Description = "Ready-message byte limit for the sample source and quarantine queues",
    DefaultValueFactory = _ => 16 * 1024 * 1024
};

RootCommand rootCommand = new("RabbitMQ Order Subscriber Sample")
{
    connectionStringOption,
    hostsOption,
    topicOption,
    durableOption,
    delayedOption,
    acknowledgmentStrategyOption,
    prefetchCountOption,
    deliveryLimitOption,
    subscriberCountOption,
    groupIdOption,
    logLevelOption, queueTypeOption, requiredOption, deadLetterOption, provisionOption, failEveryOption, maxBytesOption
};

rootCommand.SetAction(RunSubscriberAsync);
return await rootCommand.Parse(args).InvokeAsync();

async Task RunSubscriberAsync(ParseResult parseResult)
{
    string? connectionString = parseResult.GetValue(connectionStringOption);
    string? hosts = parseResult.GetValue(hostsOption);
    string? topic = parseResult.GetValue(topicOption);
    bool durable = parseResult.GetValue(durableOption);
    bool delayed = parseResult.GetValue(delayedOption);
    string? acknowledgmentStrategy = parseResult.GetValue(acknowledgmentStrategyOption);
    ushort prefetchCount = parseResult.GetValue(prefetchCountOption);
    long deliveryLimit = parseResult.GetValue(deliveryLimitOption);
    int subscriberCount = parseResult.GetValue(subscriberCountOption);
    string? groupId = parseResult.GetValue(groupIdOption);
    LogLevel logLevel = parseResult.GetValue(logLevelOption);

    string? queueType = parseResult.GetValue(queueTypeOption);
    bool required = parseResult.GetValue(requiredOption);
    string? deadLetterExchange = parseResult.GetValue(deadLetterOption);
    bool provision = parseResult.GetValue(provisionOption);
    int failEvery = parseResult.GetValue(failEveryOption);
    long maxBytes = parseResult.GetValue(maxBytesOption);
    if (queueType is not ("classic" or "quorum"))
        throw new ArgumentException("Queue type must be classic or quorum.");
    if ((String.Equals(queueType, "quorum", StringComparison.Ordinal) || required) && !durable)
        throw new ArgumentException("Quorum and required-processing examples require --durable.");
    if (acknowledgmentStrategy is not ("automatic" or "fireandforget"))
        throw new ArgumentException("Acknowledgment strategy must be automatic or fireandforget.");
    if (required && (!String.Equals(acknowledgmentStrategy, "automatic", StringComparison.Ordinal) || String.IsNullOrWhiteSpace(deadLetterExchange)))
        throw new ArgumentException("Required processing needs --acknowledgment-strategy automatic and --dead-letter-exchange.");
    ArgumentOutOfRangeException.ThrowIfNegative(failEvery);
    ArgumentOutOfRangeException.ThrowIfLessThan(subscriberCount, 1);
    ArgumentOutOfRangeException.ThrowIfLessThan(maxBytes, 1L);

    ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
    ArgumentException.ThrowIfNullOrWhiteSpace(topic);
    ArgumentException.ThrowIfNullOrWhiteSpace(groupId);

    var otlpEndpoint = Environment.GetEnvironmentVariable("OTEL_EXPORTER_OTLP_ENDPOINT");
    var serviceName = Environment.GetEnvironmentVariable("OTEL_SERVICE_NAME") ?? "subscriber";
    var resourceBuilder = ResourceBuilder.CreateDefault().AddService(serviceName);

    TracerProvider? tracerProvider = null;
    MeterProvider? meterProvider = null;

    if (!string.IsNullOrEmpty(otlpEndpoint))
    {
        tracerProvider = Sdk.CreateTracerProviderBuilder()
            .SetResourceBuilder(resourceBuilder)
            .AddSource("Foundatio", "RabbitMQ.Client.*")
            .AddOtlpExporter()
            .Build();

        meterProvider = Sdk.CreateMeterProviderBuilder()
            .SetResourceBuilder(resourceBuilder)
            .AddMeter("Foundatio")
            .AddRuntimeInstrumentation()
            .AddOtlpExporter()
            .Build();
    }

    using var tracerDisposable = tracerProvider;
    using var meterDisposable = meterProvider;

    using ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
    {
        builder.AddConsole().SetMinimumLevel(logLevel);

        if (!string.IsNullOrEmpty(otlpEndpoint))
        {
            builder.AddOpenTelemetry(otel =>
            {
                otel.SetResourceBuilder(resourceBuilder);
                otel.IncludeFormattedMessage = true;
                otel.IncludeScopes = true;
                otel.AddOtlpExporter();
            });
        }
    });
    var logger = loggerFactory.CreateLogger("Subscriber");

    if (delayed)
    {
        Uri uri = new(connectionString);
        connectionString = new UriBuilder(uri) { Port = 5673 }.Uri.ToString();
    }

    var ackStrategy = String.Equals("automatic", acknowledgmentStrategy, StringComparison.OrdinalIgnoreCase)
        ? AcknowledgementStrategy.Automatic
        : AcknowledgementStrategy.FireAndForget;

    // Parse hosts into a list if provided
    List<string> hostsList = [];
    if (!String.IsNullOrEmpty(hosts))
    {
        hostsList.AddRange(hosts.Split(',', StringSplitOptions.RemoveEmptyEntries)
                               .Select(h => h.Trim()));
    }

    logger.LogInformation("Config: Topic={Topic}, Durable={Durable}, AckStrategy={AckStrategy}, PrefetchCount={PrefetchCount}, DeliveryLimit={DeliveryLimit}, SubscriberCount={SubscriberCount}, GroupId={GroupId}",
        topic, durable, ackStrategy, prefetchCount, deliveryLimit, subscriberCount, groupId);
    if (hostsList.Count > 0)
        logger.LogInformation("Hosts: {Hosts}", String.Join(", ", hostsList));

    if (provision)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(deadLetterExchange);
        using var setup = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var uri = new Uri(connectionString);
        var factory = new ConnectionFactory { Uri = uri };
        var endpoints = RabbitMQEndpointResolver.CreateEndpoints(factory, hostsList);
        await using var connection = await factory.CreateConnectionAsync(endpoints, setup.Token);
        await using var channel = await connection.CreateChannelAsync(cancellationToken: setup.Token);
        await channel.ExchangeDeclareAsync(deadLetterExchange, "direct", true, cancellationToken: setup.Token);
        await channel.QueueDeclareAsync($"{deadLetterExchange}-queue", true, false, false,
            new Dictionary<string, object?> { [RabbitMQConstants.QueueTypeArgument] = queueType, [RabbitMQConstants.MaxLengthBytesArgument] = maxBytes, [RabbitMQConstants.OverflowArgument] = "reject-publish" },
            cancellationToken: setup.Token);
        await channel.QueueBindAsync($"{deadLetterExchange}-queue", deadLetterExchange, "quarantine", cancellationToken: setup.Token);
    }

    var messageBuses = new List<IMessageBus>(subscriberCount);
    var subscriptions = new List<Task>(subscriberCount);
    int totalProcessed = 0;

    try
    {
        for (int i = 0; i < subscriberCount; i++)
        {
            int subscriberId = i + 1;
            string queueName = durable
                ? $"{groupId}-{nameof(OrderEvent).ToLower()}"
                : $"{groupId}-{nameof(OrderEvent).ToLower()}-{Guid.NewGuid():N}";

            RabbitMQMessageBusOptions options = new()
            {
                ConnectionString = connectionString,
                Hosts = hostsList,
                Topic = topic,
                AcknowledgementStrategy = ackStrategy,
                IsDurable = durable,
                SubscriptionQueueName = queueName,
                IsSubscriptionQueueExclusive = !durable,
                SubscriptionQueueAutoDelete = !durable,
                PrefetchCount = prefetchCount,
                DeliveryLimit = deliveryLimit,
                LoggerFactory = loggerFactory,
                RequireSuccessfulDispatch = required,
                DeadLetterExchange = deadLetterExchange,
                DeadLetterRoutingKey = "quarantine",
                Overflow = QueueOverflowBehavior.RejectPublish,
                DeadLetterStrategy = String.Equals(queueType, "quorum", StringComparison.Ordinal) && !String.IsNullOrWhiteSpace(deadLetterExchange) ? DeadLetterStrategy.AtLeastOnce : null,
                Arguments = new Dictionary<string, object?> { [RabbitMQConstants.QueueTypeArgument] = queueType, [RabbitMQConstants.MaxLengthBytesArgument] = maxBytes }
            };

            RabbitMQMessageBus messageBus = new(options);
            messageBuses.Add(messageBus);

            subscriptions.Add(messageBus.SubscribeAsync<OrderEvent>(order =>
            {
                if (failEvery > 0 && order.SequenceNumber % failEvery == 0)
                    throw new InvalidOperationException($"Synthetic failure for sample order {order.SequenceNumber}");
                int processed = Interlocked.Increment(ref totalProcessed);
                TimeSpan latency = DateTimeOffset.UtcNow - order.CreatedAt;
                logger.LogInformation(
                    "Order #{Seq} | {OrderId} | Customer: {Customer} | ${Amount} | Processed: {Processed} | Latency: {Latency}ms",
                    order.SequenceNumber, order.OrderId, order.CustomerId, order.Amount, processed, latency.TotalMilliseconds.ToString("F1"));
            }));

            logger.LogInformation("Subscriber {SubscriberId} started with queue: {QueueName}", subscriberId, queueName);
        }

        await Task.WhenAll(subscriptions);

        logger.LogInformation("Waiting for messages. Press Ctrl+C to quit...");
        using var cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            cts.Cancel();
        };

        var statsTimer = Stopwatch.StartNew();
        int lastProcessed = 0;

        try
        {
            while (!cts.Token.IsCancellationRequested)
            {
                await Task.Delay(TimeSpan.FromSeconds(10), cts.Token);
                int current = Interlocked.CompareExchange(ref totalProcessed, 0, 0);
                int delta = current - lastProcessed;
                lastProcessed = current;
                logger.LogInformation("Stats | Processed: {Total} | Last 10s: +{Delta} | Rate: {Rate}/s | Uptime: {Uptime:hh\\:mm\\:ss}",
                    current, delta, (delta / 10.0).ToString("F1"), statsTimer.Elapsed);
            }
        }
        catch (OperationCanceledException) { }
    }
    finally
    {
        logger.LogInformation("Shutting down. Total orders processed: {Count}", totalProcessed);
        foreach (var messageBus in messageBuses)
            await messageBus.DisposeAsync();

        logger.LogInformation("All subscribers stopped.");
    }
}
