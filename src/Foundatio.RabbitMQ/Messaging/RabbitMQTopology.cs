using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Foundatio.Extensions;
using Foundatio.Utility;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

namespace Foundatio.Messaging;

internal sealed class RabbitMQTopology
{
    private static readonly Version _delayedExchangePluginIncompatibleVersion = new(4, 3);
    private static readonly Version _globalQosRemovedVersion = new(4, 3);
    private readonly RabbitMQMessageBusOptions _options;
    private readonly ILogger _logger;
    private bool? _delayedExchangePluginEnabled;
    private readonly bool _isQuorumQueue;

    internal bool IsQuorumQueue => _isQuorumQueue;
    internal bool SupportsDelayedExchange => _delayedExchangePluginEnabled is true;

    internal RabbitMQTopology(RabbitMQMessageBusOptions options, ILogger logger)
    {
        _options = options;
        _logger = logger;
        _isQuorumQueue = RabbitMQMessageBusOptions.IsQuorumQueue(options.Arguments);
    }

    internal Version? DetectServerVersion(IConnection connection)
    {
        var version = ParseServerVersion(connection.ServerProperties);
        if (version is not null)
            _logger.LogDebug("Connected to RabbitMQ server version {ServerVersion}", version);
        return version;
    }

    /// <summary>Parse the broker version from the UTF-8 AMQP server property.</summary>
    internal static Version? ParseServerVersion(IDictionary<string, object?>? serverProperties)
    {
        if (serverProperties?.TryGetValue("version", out var value) is not true || value is not byte[] bytes)
            return null;
        return Version.TryParse(Encoding.UTF8.GetString(bytes), out var version) ? version : null;
    }

    internal async Task<bool?> CreateDelayedExchangeAsync(IChannel channel, Version? serverVersion, CancellationToken cancellationToken)
    {
        // RabbitMQ 4.3 removed the Mnesia store required by the archived plugin.
        // Probe earlier brokers (and unknown versions) to retain 4.2 delayed delivery.
        if (serverVersion is not null && serverVersion >= _delayedExchangePluginIncompatibleVersion)
        {
            if (_delayedExchangePluginEnabled is not false)
                _logger.LogWarning("The delayed-message exchange plugin is incompatible with RabbitMQ {ServerVersion}; delayed publications use in-process scheduling unless broker delivery is required", serverVersion);
            _delayedExchangePluginEnabled = false;
            return null;
        }
        try
        {
            await channel.ExchangeDeclareAsync(_options.Topic, "x-delayed-message", _options.IsDurable, false,
                new Dictionary<string, object?> { [RabbitMQConstants.DelayedExchangeTypeArgument] = ExchangeType.Fanout }, cancellationToken: cancellationToken).AnyContext();
            if (_delayedExchangePluginEnabled is not true)
                _logger.LogWarning("The delayed-message exchange plugin is archived and incompatible with RabbitMQ 4.3+; review its performance and migration guidance before upgrading the broker");
            _delayedExchangePluginEnabled = true;
            return true;
        }
        catch (OperationInterruptedException exception) when (IsRegularExchangeFallback(exception))
        {
            // Unsupported plugin type, or an existing regular fanout topic, is a known
            // non-scheduling topology. Other declaration/permission/network errors propagate.
            _delayedExchangePluginEnabled = false;
            return false;
        }
    }

    private static bool IsRegularExchangeFallback(OperationInterruptedException exception)
    {
        var reason = exception.ShutdownReason;
        if (reason is null || reason.ReplyCode is not (406 or 503))
            return false;
        return reason.ReplyText.Contains("unknown exchange type 'x-delayed-message'", StringComparison.OrdinalIgnoreCase)
            || (reason.ReplyCode == 406
                && reason.ReplyText.Contains("inequivalent arg 'type'", StringComparison.OrdinalIgnoreCase)
                && reason.ReplyText.Contains("current is 'fanout'", StringComparison.OrdinalIgnoreCase));
    }

    internal Task DeclareSubscriptionExchangeAsync(IChannel channel, CancellationToken cancellationToken) =>
        _delayedExchangePluginEnabled is true
            ? channel.ExchangeDeclareAsync(_options.Topic, "x-delayed-message", _options.IsDurable, false,
                new Dictionary<string, object?> { [RabbitMQConstants.DelayedExchangeTypeArgument] = ExchangeType.Fanout }, cancellationToken: cancellationToken)
            : CreateRegularExchangeAsync(channel, cancellationToken);

    internal Task CreateRegularExchangeAsync(IChannel channel, CancellationToken cancellationToken) =>
        channel.ExchangeDeclareAsync(_options.Topic, ExchangeType.Fanout, _options.IsDurable, false, cancellationToken: cancellationToken);

    internal async Task<string> CreateQueueAsync(IChannel channel, Version? serverVersion, CancellationToken cancellationToken)
    {
        var arguments = _options.Arguments is not null
            ? new Dictionary<string, object?>(_options.Arguments) : new Dictionary<string, object?>();
        bool isQuorumQueue = RabbitMQMessageBusOptions.IsQuorumQueue(arguments);
        if (isQuorumQueue != _isQuorumQueue)
            throw new InvalidOperationException("Queue type cannot change after the message bus is constructed.");
        if (isQuorumQueue && _options.MaxPriority.HasValue)
            throw new InvalidOperationException("MaxPriority applies only to classic queues and cannot be used with quorum queues.");

        if (_isQuorumQueue && !arguments.ContainsKey(RabbitMQConstants.DeliveryLimitArgument))
            arguments[RabbitMQConstants.DeliveryLimitArgument] = _options.DeliveryLimit;
        if (!String.IsNullOrWhiteSpace(_options.DeadLetterExchange))
        {
            arguments[RabbitMQConstants.DeadLetterExchangeArgument] = _options.DeadLetterExchange;
            if (_options.DeadLetterRoutingKey is not null)
                arguments[RabbitMQConstants.DeadLetterRoutingKeyArgument] = _options.DeadLetterRoutingKey;
            if (_options.DeadLetterStrategy.HasValue)
            {
                if (_options.DeadLetterStrategy == DeadLetterStrategy.AtLeastOnce)
                {
                    if (!_isQuorumQueue)
                        throw new MessageBusException("At-least-once broker dead-lettering requires quorum queues.");
                    if (_options.Overflow != QueueOverflowBehavior.RejectPublish)
                        throw new MessageBusException("At-least-once broker dead-lettering requires RejectPublish overflow.");
                }
                arguments[RabbitMQConstants.DeadLetterStrategyArgument] = _options.DeadLetterStrategy.Value.ToEnumString();
            }
        }
        if (_options.Overflow.HasValue)
            arguments[RabbitMQConstants.OverflowArgument] = _options.Overflow.Value.ToEnumString();
        if (_options.ConsumerTimeout.HasValue)
        {
            if (!_isQuorumQueue)
                throw new MessageBusException("Per-queue consumer timeout (x-consumer-timeout) requires quorum queues (RabbitMQ 4.3+). Call UseQuorumQueues() before ConsumerTimeout().");
            if (serverVersion is not null && serverVersion < _delayedExchangePluginIncompatibleVersion)
                throw new MessageBusException(FormattableString.Invariant($"Per-queue consumer timeout (x-consumer-timeout) requires RabbitMQ 4.3+. Detected server version: {serverVersion}."));
            arguments[RabbitMQConstants.ConsumerTimeoutArgument] = (long)_options.ConsumerTimeout.Value.TotalMilliseconds;
        }
        if (_options.SingleActiveConsumer)
            arguments[RabbitMQConstants.SingleActiveConsumerArgument] = true;
        if (_options.MaxPriority.HasValue)
            arguments[RabbitMQConstants.MaxPriorityArgument] = (int)_options.MaxPriority.Value;
        if (_options.DelayedRetryType.HasValue)
        {
            if (!_isQuorumQueue)
                throw new MessageBusException("Delayed retries (x-delayed-retry-*) require quorum queues (RabbitMQ 4.3+). Call UseQuorumQueues() before UseDelayedRetries().");
            if (serverVersion is not null && serverVersion < _delayedExchangePluginIncompatibleVersion)
                throw new MessageBusException(FormattableString.Invariant($"Delayed retries (x-delayed-retry-*) require RabbitMQ 4.3+. Detected server version: {serverVersion}."));
            arguments[RabbitMQConstants.DelayedRetryTypeArgument] = _options.DelayedRetryType.Value.ToEnumString();
            if (_options.DelayedRetryMin.HasValue)
                arguments[RabbitMQConstants.DelayedRetryMinArgument] = _options.DelayedRetryMin.Value;
            if (_options.DelayedRetryMax.HasValue)
                arguments[RabbitMQConstants.DelayedRetryMaxArgument] = _options.DelayedRetryMax.Value;
        }
        var queue = await channel.QueueDeclareAsync(_options.SubscriptionQueueName, _options.IsDurable,
            _options.IsSubscriptionQueueExclusive, _options.SubscriptionQueueAutoDelete,
            arguments.Count > 0 ? arguments : null, cancellationToken: cancellationToken).AnyContext();
        await channel.QueueBindAsync(queue.QueueName, _options.Topic, String.Empty, cancellationToken: cancellationToken).AnyContext();
        return queue.QueueName;
    }

    internal async Task ConfigurePrefetchAsync(IChannel channel, Version? serverVersion, CancellationToken cancellationToken)
    {
        if (_options.PrefetchCount > 0 || _options.PrefetchSize > 0)
        {
#pragma warning disable CS0618
            bool global = _options.GlobalQos;
#pragma warning restore CS0618
            if (global && (_isQuorumQueue || (serverVersion is not null && serverVersion >= _globalQosRemovedVersion)))
            {
                _logger.LogWarning("Global QoS is unavailable; using per-consumer prefetch");
                global = false;
            }
            await channel.BasicQosAsync(_options.PrefetchSize, _options.PrefetchCount, global, cancellationToken).AnyContext();
        }
    }
}
