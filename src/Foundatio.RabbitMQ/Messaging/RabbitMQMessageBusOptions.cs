using System;
using System.Collections.Generic;
using Foundatio.Utility;

namespace Foundatio.Messaging;

public class RabbitMQMessageBusOptions : SharedMessageBusOptions
{
    /// <summary>
    /// The connection URI. Provides credentials and vhost. A nonempty Hosts list replaces the URI host.
    /// The amqps scheme enables TLS for every endpoint; each certificate identity must match its endpoint.
    /// </summary>
    public string? ConnectionString { get; set; }

    /// <summary>
    /// Replacement endpoints for connection establishment and network recovery, in hostname,
    /// hostname:port, bare IPv6, or [IPv6]:port form. Selection is randomized, not ordered.
    /// An omitted host-list port uses 5672 for amqp or 5671 for amqps, not a custom URI port.
    /// Explicit ports must be 1-65535. A null or empty list uses the URI endpoint.
    /// A nonempty list containing no usable endpoint is rejected.
    /// </summary>
    public IList<string>? Hosts { get; set; }

    /// <summary>
    /// The default message time to live. The value of the expiration field describes the TTL period in milliseconds.
    /// </summary>
    public TimeSpan? DefaultMessageTimeToLive { get; set; }

    /// <summary>
    /// Arguments passed to QueueDeclare. Configure this mutable dictionary before constructing
    /// the bus; changing it while the bus is running is unsupported.
    /// </summary>
    public IDictionary<string, object?>? Arguments { get; set; }

    /// <summary>
    /// Durable (will survive a broker restart)
    /// </summary>
    public bool IsDurable { get; set; } = true;

    /// <summary>
    /// Whether the subscription queue is exclusive to this message bus instance.
    /// </summary>
    public bool IsSubscriptionQueueExclusive { get; set; } = true;

    /// <summary>
    /// Whether the subscription queue should be automatically deleted.
    /// </summary>
    public bool SubscriptionQueueAutoDelete { get; set; } = true;

    /// <summary>
    /// The name of the subscription queue this message bus instance will listen on.
    /// </summary>
    public string SubscriptionQueueName { get; set; } = String.Empty;

    /// <summary>
    /// The default is FireAndForget (broker automatic acknowledgement), not acknowledgement
    /// after successful application processing. Automatic acknowledges after dispatch succeeds.
    /// </summary>
    public AcknowledgementStrategy AcknowledgementStrategy { get; set; } = AcknowledgementStrategy.FireAndForget;

    /// <summary>
    /// Limits unacknowledged deliveries per consumer, not handler concurrency.
    /// Does not bound FireAndForget consumers. When both prefetch settings are zero,
    /// no BasicQos is sent and a broker-configured default can still apply.
    /// </summary>
    public ushort PrefetchCount { get; set; }

    /// <summary>Prefetch size in bytes. Zero is the supported RabbitMQ default; not an application memory limit.</summary>
    public uint PrefetchSize { get; set; }

    /// <summary>
    /// Whether prefetch is shared by consumers on a channel, not across the connection.
    /// Unsupported for quorum queues. The provider falls back to per-consumer QoS where necessary.
    /// </summary>
    [Obsolete("Global QoS is deprecated in RabbitMQ 4.3+ and will be removed in a future version. Use per-consumer prefetch (GlobalQos = false) instead.")]
    public bool GlobalQos { get; set; }

    /// <summary>
    /// Maximum failed redeliveries after the initial attempt; -1 means unlimited.
    /// Classic retries use a confirmed, subscription-local handoff. On exhaustion, a configured
    /// dead-letter exchange receives a confirmed handoff; without one, the original is retained
    /// unless DiscardOnDeliveryLimit is explicitly enabled. Broker limits and policies are separate:
    /// a finite quorum broker delivery limit can also act on connection-loss redeliveries.
    /// </summary>
    public long DeliveryLimit { get; set; } = 2;

    /// <summary>
    /// When true, PublishAsync waits for broker confirmation before returning.
    /// This guarantees the message reached the broker but adds latency per publish.
    /// Performance impact varies by workload - use async/pipelining patterns for best results.
    /// Default: false (fire-and-forget publishing for backward compatibility).
    /// See: https://www.rabbitmq.com/docs/confirms#publisher-confirms
    /// A confirmation does not guarantee consumer processing or routing to every subscription.
    /// </summary>
    public bool PublisherConfirmsEnabled { get; set; }

    /// <summary>
    /// Require at least one route for an ordinary immediate publication. Enables confirms and
    /// AMQP mandatory returns. Does not verify all expected fanout subscriptions or consumer processing.
    /// Scheduled publication through the delayed-exchange plugin cannot satisfy this contract.
    /// </summary>
    public bool RequirePublishRouting { get; set; }

    /// <summary>
    /// Require actual completion of matching live handlers before acknowledgement. Requires Automatic,
    /// a configured DeadLetterExchange, and DiscardOnDeliveryLimit=false. Provision the destination separately.
    /// Unmatched types and malformed typed payloads become terminal failures instead of intentional
    /// pub/sub filtering. False preserves the existing best-effort filtering/dispatch contract.
    /// </summary>
    public bool RequireSuccessfulDispatch { get; set; }

    /// <summary>
    /// Reject delayed publication when broker-side delayed delivery is unavailable; never silently
    /// schedule that work in process memory. Requires durable publications and publisher confirms.
    /// This is not a guarantee of replicated scheduling or eventual destination availability.
    /// </summary>
    public bool RequireBrokerDelayedDelivery { get; set; }

    /// <summary>
    /// Explicitly opt into the legacy discard-on-exhaustion behavior when no dead-letter exchange
    /// is configured. Default: false. Discard is logged and must not be used for required work.
    /// </summary>
    public bool DiscardOnDeliveryLimit { get; set; }

    /// <summary>
    /// Maximum individual transport cleanup wait, including lock acquisition. Cleanup can continue after
    /// the wait expires; this is not a total application shutdown deadline. Handlers receive cancellation; handlers that
    /// ignore cancellation may continue running and must tolerate redelivery. Default: ten seconds.
    /// </summary>
    public TimeSpan ShutdownTimeout { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Maximum time each publish attempt will wait for connection recovery before failing.
    /// During a transient connection drop, publishes suspend (rather than failing immediately)
    /// and resume automatically when the connection recovers.
    /// Note: The resilience policy may retry failed attempts (default: 3 attempts with exponential backoff),
    /// so total wall-clock time can exceed this value.
    /// Set to TimeSpan.Zero to disable (fail immediately on connection drop, like pre-fix behavior).
    /// Default: 10 seconds (covers one full NetworkRecoveryInterval cycle with margin).
    /// </summary>
    public TimeSpan PublishRecoveryTimeout { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Heartbeat timeout negotiated with the broker. Controls how quickly dead TCP connections are detected.
    /// Lower values detect failures faster but may cause false positives on congested networks.
    /// TimeSpan.Zero disables heartbeats only when the broker also offers zero.
    /// Default: null (uses client library default of 60 seconds).
    /// See: https://www.rabbitmq.com/docs/heartbeats
    /// </summary>
    public TimeSpan? RequestedHeartbeat { get; set; }

    /// <summary>
    /// Time between automatic connection recovery attempts after a network failure.
    /// Higher values reduce reconnection pressure on the broker during outages but increase downtime.
    /// Default: null (uses client library default of 5 seconds).
    /// See: https://www.rabbitmq.com/client-libraries/dotnet-api-guide#connection-recovery
    /// </summary>
    public TimeSpan? NetworkRecoveryInterval { get; set; }

    /// <summary>
    /// Exchange used for terminal handoffs and the broker x-dead-letter-exchange argument.
    /// Provision its durable destination separately. Client terminal handoffs are confirmed and
    /// mandatory; broker-triggered expiry or delivery limits have their own dead-letter safety policy.
    /// </summary>
    public string? DeadLetterExchange { get; set; }

    /// <summary>
    /// Routing key used when dead-lettering messages. If not set, the original routing key is preserved.
    /// Only effective when DeadLetterExchange is also set.
    /// Set via the x-dead-letter-routing-key queue argument.
    /// </summary>
    public string? DeadLetterRoutingKey { get; set; }

    /// <summary>
    /// Broker dead-letter strategy for quorum queues. AtLeastOnce requires RejectPublish overflow,
    /// a configured DLX, and the broker prerequisites. It retains transfers until confirmed;
    /// duplicate transfers remain possible. AtMostOnce can lose messages in transit.
    /// </summary>
    public DeadLetterStrategy? DeadLetterStrategy { get; set; }

    /// <summary>
    /// Queue overflow behavior when the queue reaches its max length.
    /// Must be set to RejectPublish when using at-least-once dead-lettering.
    /// Set via the x-overflow queue argument.
    /// See: https://www.rabbitmq.com/docs/maxlength#overflow-behaviour
    /// </summary>
    public QueueOverflowBehavior? Overflow { get; set; }

    /// <summary>
    /// Consumer timeout in milliseconds for quorum queues (RabbitMQ 4.3+).
    /// Limits how long a consumer can hold unacknowledged messages before the broker returns them.
    /// When exceeded, messages are requeued and the consumer is cancelled gracefully.
    /// Set via the x-consumer-timeout queue argument.
    /// Default: null (uses broker default, typically 30 minutes).
    /// See: https://www.rabbitmq.com/docs/consumers#acknowledgement-timeout
    /// </summary>
    public TimeSpan? ConsumerTimeout { get; set; }

    /// <summary>
    /// Enable one active consumer with standby consumers. Does not eliminate duplicates,
    /// guarantee business execution order, or override message priorities.
    /// </summary>
    public bool SingleActiveConsumer { get; set; }

    /// <summary>
    /// Classic queue maximum priority (1-255), sent as x-max-priority. Higher limits cost
    /// more broker CPU and memory; UseMessagePriority() limits its convenience API to 32.
    /// Quorum queues cannot use this setting: RabbitMQ 4.2 uses normal/high tiers, and 4.3+
    /// has 32 strict levels automatically. RabbitMQ.Client 7.2.2 omits a message priority of zero.
    /// See: https://www.rabbitmq.com/docs/priority
    /// </summary>
    public byte? MaxPriority
    {
        get;
        set
        {
            if (value is 0)
                throw new ArgumentOutOfRangeException(nameof(MaxPriority), value, "Classic queue maximum priority must be positive.");

            field = value;
        }
    }

    /// <summary>
    /// Checks whether the supplied declaration arguments explicitly request a quorum queue.
    /// This does not discover a broker or virtual-host default, inspect an existing queue,
    /// or validate other argument types and values.
    /// </summary>
    internal static bool IsQuorumQueue(IDictionary<string, object?>? arguments)
    {
        return arguments is not null && arguments.TryGetValue(RabbitMQConstants.QueueTypeArgument, out object? queueType)
            && queueType is string type && String.Equals(type, "quorum", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Configures native delayed retry for quorum queues (RabbitMQ 4.3+).
    /// When set, rejected/failed messages are held in a delayed state before becoming available again.
    /// The delay uses linear backoff: min(min_delay * delivery_count, max_delay).
    /// Requires quorum queues. Set via x-delayed-retry-type queue argument.
    /// Default: null (not configured).
    /// See: https://www.rabbitmq.com/docs/quorum-queues#delayed-retries
    /// </summary>
    public DelayedRetryType? DelayedRetryType { get; set; }

    /// <summary>
    /// Minimum delay in milliseconds for native delayed retry (RabbitMQ 4.3+).
    /// The actual delay is: min(DelayedRetryMin * delivery_count, DelayedRetryMax).
    /// Only effective when DelayedRetryType is set.
    /// Set via x-delayed-retry-min queue argument.
    /// </summary>
    public int? DelayedRetryMin { get; set; }

    /// <summary>
    /// Maximum delay in milliseconds for native delayed retry (RabbitMQ 4.3+).
    /// Caps the linear backoff so delays don't grow unbounded.
    /// Only effective when DelayedRetryType is set.
    /// Set via x-delayed-retry-max queue argument.
    /// </summary>
    public int? DelayedRetryMax { get; set; }

    internal static void Validate(RabbitMQMessageBusOptions options)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(options.DeliveryLimit, -1L);
        ArgumentOutOfRangeException.ThrowIfLessThan(options.PublishRecoveryTimeout, TimeSpan.Zero);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(options.ShutdownTimeout, TimeSpan.Zero);
        if (options.RequestedHeartbeat.HasValue)
            ArgumentOutOfRangeException.ThrowIfLessThan(options.RequestedHeartbeat.Value, TimeSpan.Zero);
        if (options.NetworkRecoveryInterval.HasValue)
            ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(options.NetworkRecoveryInterval.Value, TimeSpan.Zero);
        if (options.MaxPriority.HasValue && IsQuorumQueue(options.Arguments))
            throw new InvalidOperationException("MaxPriority applies only to classic queues and cannot be used with quorum queues.");
        if (options.RequireSuccessfulDispatch && options.AcknowledgementStrategy != AcknowledgementStrategy.Automatic)
            throw new ArgumentException("RequireSuccessfulDispatch requires Automatic acknowledgements.", nameof(options));
        if (options.RequireSuccessfulDispatch && options.DiscardOnDeliveryLimit)
            throw new ArgumentException("Required dispatch cannot enable discard on delivery exhaustion.", nameof(options));
        if (options.RequireSuccessfulDispatch && String.IsNullOrWhiteSpace(options.DeadLetterExchange))
            throw new ArgumentException("RequireSuccessfulDispatch requires a configured DeadLetterExchange for terminal deliveries.", nameof(options));
        if (options.RequireBrokerDelayedDelivery && (!options.IsDurable || (!options.PublisherConfirmsEnabled && !options.RequirePublishRouting)))
            throw new ArgumentException("Required broker-side delay needs durable publications and publisher confirms.", nameof(options));
        if (!String.IsNullOrWhiteSpace(options.DeadLetterExchange) && String.Equals(options.DeadLetterExchange, options.Topic, StringComparison.Ordinal))
            throw new ArgumentException("The terminal exchange must differ from the source fanout exchange.", nameof(options));
    }
}

public class RabbitMQMessageBusOptionsBuilder : SharedMessageBusOptionsBuilder<RabbitMQMessageBusOptions, RabbitMQMessageBusOptionsBuilder>
{
    public RabbitMQMessageBusOptionsBuilder ConnectionString(string? connectionString)
    {
        Target.ConnectionString = String.IsNullOrWhiteSpace(connectionString) ? null : connectionString;
        return this;
    }

    /// <summary>Replace the URI host with randomly selected hostnames or IPs, optionally including ports.</summary>
    public RabbitMQMessageBusOptionsBuilder Hosts(params string[] hosts)
    {
        Target.Hosts = hosts ?? throw new ArgumentNullException(nameof(hosts));
        return this;
    }

    /// <summary>Replace the URI host. An empty list uses the URI endpoint.</summary>
    public RabbitMQMessageBusOptionsBuilder Hosts(IList<string> hosts)
    {
        Target.Hosts = hosts ?? throw new ArgumentNullException(nameof(hosts));
        return this;
    }

    public RabbitMQMessageBusOptionsBuilder DefaultMessageTimeToLive(TimeSpan defaultMessageTimeToLive)
    {
        Target.DefaultMessageTimeToLive = defaultMessageTimeToLive;
        return this;
    }

    public RabbitMQMessageBusOptionsBuilder Arguments(IDictionary<string, object?> arguments)
    {
        Target.Arguments = arguments ?? throw new ArgumentNullException(nameof(arguments));
        return this;
    }

    public RabbitMQMessageBusOptionsBuilder IsDurable(bool isDurable)
    {
        Target.IsDurable = isDurable;
        return this;
    }

    public RabbitMQMessageBusOptionsBuilder IsSubscriptionQueueExclusive(bool isExclusive)
    {
        Target.IsSubscriptionQueueExclusive = isExclusive;
        return this;
    }

    public RabbitMQMessageBusOptionsBuilder SubscriptionQueueAutoDelete(bool autoDelete)
    {
        Target.SubscriptionQueueAutoDelete = autoDelete;
        return this;
    }

    public RabbitMQMessageBusOptionsBuilder SubscriptionQueueName(string subscriptionQueueName)
    {
        Target.SubscriptionQueueName = subscriptionQueueName;
        return this;
    }

    public RabbitMQMessageBusOptionsBuilder AcknowledgementStrategy(AcknowledgementStrategy acknowledgementStrategy)
    {
        Target.AcknowledgementStrategy = acknowledgementStrategy;
        return this;
    }

    public RabbitMQMessageBusOptionsBuilder PrefetchCount(ushort prefetchCount)
    {
        Target.PrefetchCount = prefetchCount;
        return this;
    }

    public RabbitMQMessageBusOptionsBuilder PrefetchSize(uint prefetchSize)
    {
        Target.PrefetchSize = prefetchSize;
        return this;
    }

    [Obsolete("Global QoS is deprecated in RabbitMQ 4.3+ and will be removed in a future version. Use per-consumer prefetch (GlobalQos = false) instead.")]
    public RabbitMQMessageBusOptionsBuilder GlobalQos(bool globalQos)
    {
#pragma warning disable CS0618
        Target.GlobalQos = globalQos;
#pragma warning restore CS0618
        return this;
    }

    public RabbitMQMessageBusOptionsBuilder DeliveryLimit(long deliveryLimit)
    {
        Target.DeliveryLimit = deliveryLimit;

        Target.Arguments ??= new Dictionary<string, object?>();
        Target.Arguments[RabbitMQConstants.DeliveryLimitArgument] = deliveryLimit;

        return this;
    }

    /// <summary>Enable broker confirmation, not consumer-processing confirmation.</summary>
    public RabbitMQMessageBusOptionsBuilder PublisherConfirmsEnabled(bool enabled = true)
    {
        Target.PublisherConfirmsEnabled = enabled;
        return this;
    }

    /// <summary>Require a confirmed, routed immediate publication; scheduled routing is not covered.</summary>
    public RabbitMQMessageBusOptionsBuilder RequirePublishRouting(bool required = true)
    {
        Target.RequirePublishRouting = required;
        return this;
    }

    /// <summary>Require matching live handlers to complete successfully before acknowledgement.</summary>
    public RabbitMQMessageBusOptionsBuilder RequireSuccessfulDispatch(bool required = true)
    {
        Target.RequireSuccessfulDispatch = required;
        return this;
    }

    /// <summary>Reject delayed publications instead of using an in-memory fallback.</summary>
    public RabbitMQMessageBusOptionsBuilder RequireBrokerDelayedDelivery(bool required = true)
    {
        Target.RequireBrokerDelayedDelivery = required;
        return this;
    }

    /// <summary>Explicitly allow discarding exhausted deliveries without a configured DLX.</summary>
    public RabbitMQMessageBusOptionsBuilder DiscardOnDeliveryLimit(bool discard = true)
    {
        Target.DiscardOnDeliveryLimit = discard;
        return this;
    }

    /// <summary>Set the positive, bounded transport shutdown timeout.</summary>
    public RabbitMQMessageBusOptionsBuilder ShutdownTimeout(TimeSpan timeout)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(timeout, TimeSpan.Zero);
        Target.ShutdownTimeout = timeout;
        return this;
    }

    /// <summary>
    /// Sets the maximum time each publish attempt will wait for connection recovery before failing.
    /// During a transient connection drop, publishes suspend and resume automatically on recovery.
    /// Set to TimeSpan.Zero to disable waiting (fail immediately on connection drop).
    /// </summary>
    /// <param name="timeout">Maximum per-attempt recovery wait time. Default: 10 seconds.</param>
    /// <returns>The builder instance for method chaining.</returns>
    public RabbitMQMessageBusOptionsBuilder PublishRecoveryTimeout(TimeSpan timeout)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(timeout, TimeSpan.Zero);
        Target.PublishRecoveryTimeout = timeout;
        return this;
    }

    /// <summary>
    /// Select quorum queues and retain them across disconnects. Preserves other raw arguments.
    /// The broker delivery limit is set to the current DeliveryLimit; an explicit raw override
    /// can instead disable that broker limit (-1) while retaining the application retry budget.
    /// </summary>
    public RabbitMQMessageBusOptionsBuilder UseQuorumQueues()
    {
        if (Target.MaxPriority.HasValue)
            throw new InvalidOperationException("MaxPriority applies only to classic queues and cannot be used with quorum queues.");

        Target.SubscriptionQueueAutoDelete = false;
        Target.IsSubscriptionQueueExclusive = false;

        Target.Arguments ??= new Dictionary<string, object?>();
        Target.Arguments[RabbitMQConstants.QueueTypeArgument] = "quorum";
        Target.Arguments[RabbitMQConstants.DeliveryLimitArgument] = Target.DeliveryLimit;
        return this;
    }

    /// <summary>
    /// Sets the heartbeat timeout negotiated with the broker.
    /// Controls how quickly dead TCP connections are detected.
    /// </summary>
    /// <param name="heartbeat">Requested heartbeat timeout. Heartbeats are disabled only when both peers offer zero.</param>
    public RabbitMQMessageBusOptionsBuilder RequestedHeartbeat(TimeSpan heartbeat)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(heartbeat, TimeSpan.Zero);
        Target.RequestedHeartbeat = heartbeat;
        return this;
    }

    /// <summary>
    /// Sets the interval between automatic connection recovery attempts.
    /// </summary>
    /// <param name="interval">Recovery interval. Must be positive.</param>
    public RabbitMQMessageBusOptionsBuilder NetworkRecoveryInterval(TimeSpan interval)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(interval, TimeSpan.Zero);
        Target.NetworkRecoveryInterval = interval;
        return this;
    }

    /// <summary>Configure terminal routing. Provision the durable destination separately.</summary>
    public RabbitMQMessageBusOptionsBuilder DeadLetterExchange(string exchange, string? routingKey = null, DeadLetterStrategy? strategy = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(exchange);
        Target.DeadLetterExchange = exchange;
        Target.DeadLetterRoutingKey = routingKey;
        Target.DeadLetterStrategy = strategy;
        return this;
    }

    /// <summary>
    /// Sets the queue overflow behavior when max length is reached.
    /// Must be RejectPublish when using at-least-once dead-lettering on quorum queues.
    /// </summary>
    /// <param name="behavior">The overflow behavior.</param>
    public RabbitMQMessageBusOptionsBuilder OverflowBehavior(QueueOverflowBehavior behavior)
    {
        Target.Overflow = behavior;
        return this;
    }

    /// <summary>
    /// Sets the consumer timeout for quorum queues (RabbitMQ 4.3+).
    /// When a consumer holds unacknowledged messages longer than this, the broker returns them
    /// and gracefully cancels the consumer.
    /// </summary>
    /// <param name="timeout">Timeout duration. Must be positive.</param>
    public RabbitMQMessageBusOptionsBuilder ConsumerTimeout(TimeSpan timeout)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(timeout, TimeSpan.Zero);
        Target.ConsumerTimeout = timeout;
        return this;
    }

    /// <summary>Enable a single active consumer; does not eliminate redelivery or enforce business ordering.</summary>
    public RabbitMQMessageBusOptionsBuilder UseSingleActiveConsumer(bool enabled = true)
    {
        Target.SingleActiveConsumer = enabled;
        return this;
    }

    /// <summary>
    /// Sets x-max-priority for classic queues only. RabbitMQ 4.2 quorum queues use
    /// normal/high tiers; RabbitMQ 4.3+ quorum queues have 32 strict levels automatically.
    /// Cannot be combined with UseQuorumQueues().
    /// </summary>
    /// <param name="maxPriority">Classic queue maximum priority (1-32). Default: 32.</param>
    public RabbitMQMessageBusOptionsBuilder UseMessagePriority(byte maxPriority = 32)
    {
        ArgumentOutOfRangeException.ThrowIfZero(maxPriority);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(maxPriority, (byte)32);
        if (RabbitMQMessageBusOptions.IsQuorumQueue(Target.Arguments))
            throw new InvalidOperationException("MaxPriority applies only to classic queues and cannot be used with quorum queues.");

        Target.MaxPriority = maxPriority;
        return this;
    }

    /// <summary>
    /// Configures native delayed retry for quorum queues (RabbitMQ 4.3+).
    /// Rejected/failed messages are held in a delayed state with linear backoff before redelivery.
    /// This applies to rejected messages; it does not schedule initial publications.
    /// </summary>
    /// <param name="minDelayMs">Minimum delay in milliseconds (multiplied by delivery count).</param>
    /// <param name="maxDelayMs">Maximum delay cap in milliseconds.</param>
    /// <param name="retryType">Retry type controlling which messages are delayed. Default: All.</param>
    public RabbitMQMessageBusOptionsBuilder UseDelayedRetries(int minDelayMs = 1000, int maxDelayMs = 60000, DelayedRetryType retryType = DelayedRetryType.All)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(minDelayMs, 0);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(maxDelayMs, 0);

        if (maxDelayMs < minDelayMs)
            throw new ArgumentOutOfRangeException(nameof(maxDelayMs), FormattableString.Invariant($"maxDelayMs ({maxDelayMs}) must be >= minDelayMs ({minDelayMs})"));

        Target.DelayedRetryType = retryType;
        Target.DelayedRetryMin = minDelayMs;
        Target.DelayedRetryMax = maxDelayMs;
        return this;
    }
}
