using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Foundatio.AsyncEx;
using Foundatio.Extensions;
using Foundatio.Serializer;
using Foundatio.Utility;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

namespace Foundatio.Messaging;

public class RabbitMQMessageBus : MessageBusBase<RabbitMQMessageBusOptions>
{
    private static readonly Func<ILogger, string?, ulong, IDisposable?> _deliveryScope =
        LoggerMessage.DefineScope<string?, ulong>("Message {MessageId}, delivery {DeliveryTag}");
    private readonly CancellationToken _shutdownToken;
    private readonly RabbitMQTopology _topology;
    private readonly Func<IMessage, object?> _deserializeMessageBody;
    private readonly AsyncLock _lock = new();
    private readonly AsyncManualResetEvent _publisherReady = new(true);
    private readonly ConnectionFactory _factory;
    private readonly List<AmqpTcpEndpoint> _endpoints;
    private IConnection? _publisherConnection;
    private IConnection? _subscriberConnection;
    private volatile IChannel? _publisherChannel;
    private volatile IChannel? _subscriberChannel;
    private AsyncEventingBasicConsumer? _consumer;
    private volatile bool _isPublisherBlocked;
    private volatile string? _publisherBlockedReason;
    private readonly AsyncLock _subscriberLock = new();
    private readonly CancellationTokenSource _shutdown = new();
    private readonly ConcurrentDictionary<string, CancellationTokenRegistration> _subscriberRegistrations = new();
    private readonly object _maintenanceSync = new();
    private readonly AsyncAutoResetEvent _subscriptionChanged = new();
    private Task? _maintenanceTask;
    private Task? _shutdownCancellation;
    private DeliveryEpoch _deliveryEpoch;
    private volatile bool _subscriberRecovering;
    private volatile bool _permanentSubscriberFault;
    private volatile Exception? _subscriptionError;
    private volatile string? _subscriptionQueueName;
    private readonly AsyncLock _handoffLock = new();
    private IConnection? _handoffConnection;
    private IChannel? _handoffChannel;
    private int _blockedDeliveries;
    private int _activeDeliveries;
    private volatile Exception? _deliveryError;

    public RabbitMQMessageBus(RabbitMQMessageBusOptions options) : base(options)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(options.ConnectionString, nameof(options.ConnectionString));
        if (!Uri.TryCreate(options.ConnectionString, UriKind.Absolute, out var primaryUri))
            throw new ArgumentException("ConnectionString is not a valid URI.", nameof(options.ConnectionString));
        if (!primaryUri.Scheme.Equals("amqp", StringComparison.OrdinalIgnoreCase) &&
            !primaryUri.Scheme.Equals("amqps", StringComparison.OrdinalIgnoreCase))
            throw new ArgumentException("ConnectionString must use amqp:// or amqps://.", nameof(options.ConnectionString));

        RabbitMQMessageBusOptions.Validate(options);
        _topology = new RabbitMQTopology(options, _logger);
        _deserializeMessageBody = DeserializeMessageBody;
        _factory = new ConnectionFactory { Uri = primaryUri, AutomaticRecoveryEnabled = true };
        if (options.RequestedHeartbeat.HasValue)
            _factory.RequestedHeartbeat = options.RequestedHeartbeat.Value;
        if (options.NetworkRecoveryInterval.HasValue)
            _factory.NetworkRecoveryInterval = options.NetworkRecoveryInterval.Value;
        _endpoints = RabbitMQEndpointResolver.CreateEndpoints(_factory, options.Hosts);
        _shutdownToken = _shutdown.Token;
        _deliveryEpoch = new DeliveryEpoch(_shutdownToken);
    }

    public RabbitMQMessageBus(Builder<RabbitMQMessageBusOptionsBuilder, RabbitMQMessageBusOptions> config)
        : this(config(new RabbitMQMessageBusOptionsBuilder()).Build())
    {
    }

    /// <summary>Whether a live transport consumer can dispatch to registered handlers without a retained-delivery blockage.</summary>
    public bool IsSubscriptionReady => !IsDisposed && !_subscriberRecovering && !_permanentSubscriberFault
        && _subscriptionError is null && Volatile.Read(ref _blockedDeliveries) == 0 && !_subscribers.IsEmpty
        && _subscriberConnection is { IsOpen: true } && _subscriberChannel is { IsOpen: true } && _consumer is { IsRunning: true };

    /// <summary>The latest subscription initialization/recovery error, cleared after successful recovery.</summary>
    public Exception? LastSubscriptionError => _subscriptionError;

    /// <summary>
    /// The latest retained-delivery or handoff error. Retention is not successful processing.
    /// Inspect IsSubscriptionReady as well; this is not a broker queue-depth measurement.
    /// </summary>
    public Exception? LastDeliveryError => _deliveryError;

    // Test synchronization observes callback completion, not just entry into the user handler.
    internal int ActiveDeliveryCount => Volatile.Read(ref _activeDeliveries);

    protected override Task RemoveTopicSubscriptionAsync() => CleanupTransportAsync(_subscriberLock, async () =>
    {
        await ClearSubscriberChannelAsync().AnyContext();
        await ClearSubscriberConnectionAsync().AnyContext();
    }, "subscriber");

    protected override async Task ShutdownAsync()
    {
        _shutdownCancellation = _shutdown.CancelAsync();
        try
        {
            await _shutdownCancellation.WaitAsync(_options.ShutdownTimeout).AnyContext();
        }
        catch (Exception exception) when (exception is not OutOfMemoryException and not StackOverflowException)
        {
            _logger.LogWarning(exception, "Subscription cancellation did not finish cleanly");
        }
        InvalidateDeliveries();
        if (_maintenanceTask is not null)
        {
            try
            {
                await _maintenanceTask.WaitAsync(_options.ShutdownTimeout).AnyContext();
            }
            catch (Exception exception) when (exception is not OutOfMemoryException and not StackOverflowException)
            {
                _logger.LogWarning(exception, "Subscription maintenance did not stop cleanly");
            }
        }
        await base.ShutdownAsync().AnyContext();
    }

    protected override async Task CleanupAsync()
    {
        _factory.AutomaticRecoveryEnabled = false;
        await CleanupTransportAsync(_lock, ClearPublisherTransportAsync, "publisher").AnyContext();
        await CleanupTransportAsync(_handoffLock, ClearHandoffTransportAsync, "handoff").AnyContext();
        foreach (var registration in _subscriberRegistrations.Values)
            registration.Unregister();
        _subscriberRegistrations.Clear();
        _publisherReady.Set();
        _deliveryEpoch.Cancel(_logger);
        // Cancellation callbacks can be user code. Do not dispose their source while
        // they are still running, or wait forever for them on the transport cleanup path.
        _ = FinishShutdownCancellationAsync();
    }

    private async Task CleanupTransportAsync(AsyncLock mutex, Func<Task> cleanup, string role)
    {
        // Keep ownership of cleanup after the caller's deadline. A busy transport must
        // eventually be disposed without making application shutdown wait for its lock.
        var pending = CleanupWhenAvailableAsync();
        try
        {
            await pending.WaitAsync(_options.ShutdownTimeout).AnyContext();
        }
        catch (TimeoutException)
        {
            _logger.LogWarning("Transport cleanup continues after the shutdown wait ({Role})", role);
        }

        async Task CleanupWhenAvailableAsync()
        {
            try
            {
                using (await mutex.LockAsync().AnyContext())
                    await cleanup().AnyContext();
            }
            catch (Exception exception) when (exception is not OutOfMemoryException and not StackOverflowException)
            {
                _logger.LogWarning(exception, "Deferred transport cleanup failed ({Role})", role);
            }
        }
    }

    private async Task FinishShutdownCancellationAsync()
    {
        try
        {
            if (_shutdownCancellation is not null)
                await _shutdownCancellation.AnyContext();
        }
        catch (Exception exception) when (exception is not OutOfMemoryException and not StackOverflowException)
        {
            _logger.LogWarning(exception, "A shutdown cancellation callback failed");
        }
        finally
        {
            _shutdown.Dispose();
        }
    }

    protected override async Task SubscribeImplAsync<T>(Func<T, CancellationToken, Task> handler, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(handler);
        var subscriber = new Subscriber
        {
            Type = typeof(T),
            CancellationToken = cancellationToken,
            Action = async (message, token) =>
            {
                if (message is not T typed)
                {
                    if (_options.RequireSuccessfulDispatch)
                        throw new InvalidDeliveryException("The delivery cannot be assigned to the required handler.");
                    return;
                }
                using var linked = CancellationTokenSource.CreateLinkedTokenSource(token, _shutdownToken);
                await handler(typed, linked.Token).WaitAsync(linked.Token).AnyContext();
            }
        };
        if (typeof(T).IsGenericType && typeof(T).GetGenericTypeDefinition() == typeof(IMessage<>))
            subscriber.GenericType = typeof(Message<>).MakeGenericType(typeof(T).GenericTypeArguments[0]);
        if (!_subscribers.TryAdd(subscriber.Id, subscriber))
            throw new MessageBusException("Unable to register the local subscription.");

        var registration = cancellationToken.Register(() =>
        {
            _subscribers.TryRemove(subscriber.Id, out _);
            if (_subscriberRegistrations.TryRemove(subscriber.Id, out var current))
                current.Unregister();
            _subscriptionChanged.Set();
        });
        _subscriberRegistrations[subscriber.Id] = registration;
        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            _permanentSubscriberFault = false;
            lock (_maintenanceSync)
                _maintenanceTask ??= MaintainSubscriptionAsync();
            // One caller can stop waiting without cancelling setup needed by other subscribers.
            // Maintenance is already running so abandoned initialization cannot leave an orphan consumer.
            var initialization = InitializeSubscriptionAsync(DisposedCancellationToken);
            try
            {
                await initialization.WaitAsync(cancellationToken).AnyContext();
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                _ = ObserveSubscriptionInitializationAsync(initialization);
                throw;
            }
            cancellationToken.ThrowIfCancellationRequested();
        }
        catch
        {
            _subscribers.TryRemove(subscriber.Id, out _);
            _subscriberRegistrations.TryRemove(subscriber.Id, out _);
            registration.Unregister();
            _subscriptionChanged.Set();
            throw;
        }
    }

    private async Task ObserveSubscriptionInitializationAsync(Task initialization)
    {
        try
        {
            await initialization.AnyContext();
        }
        catch (OperationCanceledException) when (_shutdown.IsCancellationRequested || IsDisposed)
        {
        }
        catch (Exception exception) when (exception is not OutOfMemoryException and not StackOverflowException)
        {
            _logger.LogDebug(exception, "Subscription initialization ended after its caller stopped waiting");
        }
    }

    protected override Task EnsureTopicSubscriptionAsync(CancellationToken cancellationToken) =>
        InitializeSubscriptionAsync(cancellationToken);

    private async Task InitializeSubscriptionAsync(CancellationToken cancellationToken)
    {
        if (_subscribers.IsEmpty)
            return;
        using var setup = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _shutdownToken);
        setup.CancelAfter(TimeSpan.FromSeconds(30));
        using (await _subscriberLock.LockAsync(setup.Token).AnyContext())
        {
            if (_subscriberChannel is { IsOpen: true } && _consumer is { IsRunning: true })
                return;
            if (_subscriberRecovering)
                return;
            setup.Token.ThrowIfCancellationRequested();
            if (IsDisposed)
                throw new MessageBusException("Cannot initialize a disposed subscription.");
            try
            {
                await EnsureTopicCreatedAsync(setup.Token).AnyContext();
                await ClearSubscriberChannelAsync().AnyContext();
                if (_subscribers.IsEmpty)
                    return;
                if (_subscriberConnection is not { IsOpen: true })
                {
                    await ClearSubscriberConnectionAsync().AnyContext();
                    _subscriberConnection = await CreateConnectionAsync(setup.Token).AnyContext();
                    RegisterSubscriberConnectionEventHandlers();
                }
                var serverVersion = _topology.DetectServerVersion(_subscriberConnection);
                _subscriberChannel = await _subscriberConnection.CreateChannelAsync(cancellationToken: setup.Token).AnyContext();
                _subscriberChannel.ChannelShutdownAsync += OnSubscriberChannelShutdownAsync;
                await _topology.DeclareSubscriptionExchangeAsync(_subscriberChannel, setup.Token).AnyContext();
                _subscriptionQueueName = await _topology.CreateQueueAsync(_subscriberChannel, serverVersion, setup.Token).AnyContext();
                await _topology.ConfigurePrefetchAsync(_subscriberChannel, serverVersion, setup.Token).AnyContext();
                if (_subscribers.IsEmpty)
                {
                    await ClearSubscriberChannelAsync().AnyContext();
                    return;
                }
                _consumer = new AsyncEventingBasicConsumer(_subscriberChannel);
                _consumer.ReceivedAsync += OnMessageAsync;
                _consumer.ShutdownAsync += OnConsumerShutdownAsync;
                _consumer.UnregisteredAsync += OnConsumerUnregisteredAsync;
                var registered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                Task OnRegisteredAsync(object sender, ConsumerEventArgs args)
                {
                    registered.TrySetResult();
                    return Task.CompletedTask;
                }
                _consumer.RegisteredAsync += OnRegisteredAsync;
                try
                {
                    await _subscriberChannel.BasicConsumeAsync(_subscriptionQueueName,
                        _options.AcknowledgementStrategy == AcknowledgementStrategy.FireAndForget, _consumer, cancellationToken: setup.Token).AnyContext();
                    // The consume RPC completes before the dispatcher marks the consumer as running.
                    await registered.Task.WaitAsync(setup.Token).AnyContext();
                }
                finally
                {
                    _consumer.RegisteredAsync -= OnRegisteredAsync;
                }
                _subscriptionError = null;
                _permanentSubscriberFault = false;
            }
            catch (Exception exception)
            {
                _subscriptionError = exception;
                _permanentSubscriberFault = IsPermanentSubscriptionError(exception);
                await ClearSubscriberChannelAsync().AnyContext();
                await ClearSubscriberConnectionAsync().AnyContext();
                throw;
            }
        }
    }

    private async Task MaintainSubscriptionAsync()
    {
        try
        {
            while (!_shutdown.IsCancellationRequested)
            {
                using (var wakeup = CancellationTokenSource.CreateLinkedTokenSource(_shutdownToken))
                {
                    wakeup.CancelAfter(TimeSpan.FromSeconds(1));
                    try
                    {
                        await _subscriptionChanged.WaitAsync(wakeup.Token).AnyContext();
                    }
                    catch (OperationCanceledException) when (!_shutdown.IsCancellationRequested)
                    {
                        // Periodically check transport health even without a local subscription change.
                    }
                }
                if (IsDisposed)
                    break;
                if (_permanentSubscriberFault || _subscribers.IsEmpty)
                {
                    using (await _subscriberLock.LockAsync(_shutdownToken).AnyContext())
                    {
                        // Recheck under the lock: another caller may have registered while we waited.
                        if (_permanentSubscriberFault || _subscribers.IsEmpty)
                        {
                            await ClearSubscriberChannelAsync().AnyContext();
                            if (_permanentSubscriberFault)
                                await ClearSubscriberConnectionAsync().AnyContext();
                        }
                    }
                    continue;
                }
                if (_subscriberRecovering)
                    continue;
                try
                {
                    await InitializeSubscriptionAsync(_shutdownToken).AnyContext();
                }
                catch (OperationCanceledException) when (_shutdown.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception exception) when (exception is not OutOfMemoryException and not StackOverflowException)
                {
                    _subscriptionError = exception;
                    _logger.LogError(exception, "Subscription repair failed; permanent fault: {Permanent}", _permanentSubscriberFault);
                }
            }
        }
        catch (OperationCanceledException) when (_shutdown.IsCancellationRequested) { }
    }

    private Task OnSubscriberConnectionOnCallbackExceptionAsync(object sender, CallbackExceptionEventArgs e)
    {
        _logger.LogError(e.Exception, "Subscriber callback failed");
        return Task.CompletedTask;
    }

    private Task OnSubscriberConnectionOnConnectionRecoveryErrorAsync(object sender, ConnectionRecoveryErrorEventArgs e)
    {
        _subscriptionError = e.Exception;
        _permanentSubscriberFault = IsPermanentSubscriptionError(e.Exception);
        _logger.LogError(e.Exception, "Subscriber connection recovery failed");
        return Task.CompletedTask;
    }

    private Task OnSubscriberConnectionOnConnectionShutdownAsync(object sender, ShutdownEventArgs e)
    {
        if (e.Initiator != ShutdownInitiator.Application && !IsDisposed)
            _subscriberRecovering = true;
        InvalidateDeliveries();
        _logger.LogInformation("Subscriber connection shutdown: {ReplyCode} {ReplyText}", e.ReplyCode, e.ReplyText);
        return Task.CompletedTask;
    }

    private Task OnSubscriberConnectionOnRecoverySucceededAsync(object sender, AsyncEventArgs e)
    {
        _subscriptionError = null;
        _subscriberRecovering = false;
        _logger.LogInformation("Subscriber connection and topology recovery completed");
        return Task.CompletedTask;
    }

    private Task OnConsumerShutdownAsync(object sender, ShutdownEventArgs e)
    {
        _logger.LogInformation("Consumer channel shutdown: {ReplyCode} {ReplyText}", e.ReplyCode, e.ReplyText);
        return Task.CompletedTask;
    }

    private Task OnSubscriberChannelShutdownAsync(object sender, ShutdownEventArgs e)
    {
        InvalidateDeliveries();
        return Task.CompletedTask;
    }

    private Task OnConsumerUnregisteredAsync(object sender, ConsumerEventArgs e)
    {
        InvalidateDeliveries();
        return Task.CompletedTask;
    }

    private Task OnQueueNameChangedAfterRecoveryAsync(object sender, QueueNameChangedAfterRecoveryEventArgs e)
    {
        if (String.Equals(_subscriptionQueueName, e.NameBefore, StringComparison.Ordinal))
            _subscriptionQueueName = e.NameAfter;
        return Task.CompletedTask;
    }

    private static bool IsPermanentSubscriptionError(Exception exception) =>
        exception is OperationInterruptedException { ShutdownReason.ReplyCode: 403 or 406 or 530 }
        || (exception.InnerException is not null && IsPermanentSubscriptionError(exception.InnerException));

    private void InvalidateDeliveries()
    {
        var previous = Interlocked.Exchange(ref _deliveryEpoch, new DeliveryEpoch(_shutdownToken));
        previous.Cancel(_logger);
    }

    private async Task OnMessageAsync(object sender, BasicDeliverEventArgs envelope)
    {
        if (IsDisposed || sender is not AsyncEventingBasicConsumer consumer || !ReferenceEquals(consumer, _consumer))
            return;
        var channel = consumer.Channel;
        var epoch = Volatile.Read(ref _deliveryEpoch);
        string? queueName = _subscriptionQueueName;
        using var lifetime = envelope.CancellationToken.CanBeCanceled
            ? CancellationTokenSource.CreateLinkedTokenSource(epoch.Token, envelope.CancellationToken) : null;
        var token = lifetime?.Token ?? epoch.Token;
        using var scope = _deliveryScope(_logger, envelope.BasicProperties.MessageId, envelope.DeliveryTag);
        Interlocked.Increment(ref _activeDeliveries);
        try
        {
            // Maintenance closes an unused consumer. Until then a quick resubscription
            // may use this retained delivery; do not ACK an empty dispatch snapshot.
            while (_subscribers.IsEmpty)
                await Task.Delay(TimeSpan.FromMilliseconds(100), token).AnyContext();
            token.ThrowIfCancellationRequested();
            Exception? failure = null;
            try
            {
                var message = ConvertToMessage(envelope);
                if (_options.RequireSuccessfulDispatch)
                {
                    while (!await DispatchRequiredAsync(message, token).AnyContext())
                        await Task.Delay(TimeSpan.FromMilliseconds(100), token).AnyContext();
                }
                else
                {
                    await SendMessageToSubscribersAsync(message).WaitAsync(token).AnyContext();
                }
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
                return;
            }
            catch (Exception exception) when (exception is not OutOfMemoryException and not StackOverflowException)
            {
                failure = exception;
            }

            if (_options.AcknowledgementStrategy != AcknowledgementStrategy.Automatic)
            {
                if (failure is not null)
                    _logger.LogError(failure, "Best-effort handler failed after broker automatic acknowledgement");
                return;
            }
            if (!CanSettle(channel, epoch))
                return;
            if (failure is null)
            {
                await channel.BasicAckAsync(envelope.DeliveryTag, false, token).AnyContext();
                return;
            }

            long retryCount = RabbitMQMessageConverter.GetRetryCountFromHeader(envelope);
            bool terminal = failure is InvalidDeliveryException || retryCount == Int64.MaxValue
                || (_options.DeliveryLimit >= 0 && retryCount >= _options.DeliveryLimit);
            if (terminal)
            {
                await CompleteTerminalDeliveryAsync(envelope, channel, epoch, failure, token).AnyContext();
            }
            else if (_topology.IsQuorumQueue)
            {
                if (CanSettle(channel, epoch))
                    await channel.BasicRejectAsync(envelope.DeliveryTag, true, token).AnyContext();
            }
            else
            {
                if (String.IsNullOrEmpty(queueName))
                {
                    await RetainDeliveryAsync(new MessageBusException("The actual subscription queue is unavailable for a local retry."), token).AnyContext();
                    return;
                }
                var properties = RabbitMQMessageConverter.CopyHandoffProperties(envelope);
                properties.Headers![RabbitMQConstants.XDeliveryCountHeader] = retryCount + 1;
                await TransferAndAcknowledgeAsync(envelope, channel, epoch, String.Empty, queueName, properties, token).AnyContext();
            }
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
            // Never acknowledge a delivery whose transport generation has ended.
        }
        catch (Exception exception) when (exception is not OutOfMemoryException and not StackOverflowException)
        {
            _deliveryError = exception;
            _logger.LogError(exception, "Delivery settlement did not complete; no successful processing is asserted");
            if (CanSettle(channel, epoch))
            {
                try
                {
                    await RetainDeliveryAsync(exception, token).AnyContext();
                }
                catch (OperationCanceledException) when (token.IsCancellationRequested) { }
            }
        }
        finally
        {
            Interlocked.Decrement(ref _activeDeliveries);
        }
    }

    private bool CanSettle(IChannel channel, DeliveryEpoch epoch) =>
        !IsDisposed && !epoch.Token.IsCancellationRequested && ReferenceEquals(epoch, Volatile.Read(ref _deliveryEpoch))
        && ReferenceEquals(channel, _subscriberChannel) && channel.IsOpen;

    private async Task CompleteTerminalDeliveryAsync(BasicDeliverEventArgs envelope, IChannel channel,
        DeliveryEpoch epoch, Exception failure, CancellationToken cancellationToken)
    {
        if (!String.IsNullOrWhiteSpace(_options.DeadLetterExchange))
        {
            var properties = RabbitMQMessageConverter.CopyHandoffProperties(envelope);
            if (properties.Expiration is not null)
                properties.Headers![RabbitMQConstants.XOriginalExpirationHeader] = properties.Expiration;
            properties.Expiration = null;
            properties.Headers![RabbitMQConstants.FailureTypeHeader] = failure.GetType().Name;
            properties.Headers[RabbitMQConstants.OriginalExchangeHeader] = envelope.Exchange;
            properties.Headers[RabbitMQConstants.OriginalRoutingKeyHeader] = envelope.RoutingKey;
            await TransferAndAcknowledgeAsync(envelope, channel, epoch, _options.DeadLetterExchange,
                _options.DeadLetterRoutingKey ?? envelope.RoutingKey, properties, cancellationToken).AnyContext();
        }
        else if (_options.DiscardOnDeliveryLimit)
        {
            _logger.LogWarning("Discarding exhausted delivery because DiscardOnDeliveryLimit was explicitly enabled");
            if (CanSettle(channel, epoch))
                await channel.BasicAckAsync(envelope.DeliveryTag, false, cancellationToken).AnyContext();
        }
        else
        {
            await RetainDeliveryAsync(new MessageBusException(
                "Delivery reached its terminal outcome without a configured destination; the original remains unacknowledged.", failure), cancellationToken).AnyContext();
        }
    }

    private async Task RetainDeliveryAsync(Exception exception, CancellationToken cancellationToken)
    {
        _deliveryError = exception;
        Interlocked.Increment(ref _blockedDeliveries);
        try
        {
            _logger.LogError(exception, "Retaining delivery without acknowledgement; repair the destination/configuration before replay");
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken).AnyContext();
        }
        finally
        {
            Interlocked.Decrement(ref _blockedDeliveries);
        }
    }

    private async Task TransferAndAcknowledgeAsync(BasicDeliverEventArgs envelope, IChannel source,
        DeliveryEpoch epoch, string exchange, string routingKey, BasicProperties properties, CancellationToken cancellationToken)
    {
        // A lost confirmation can produce duplicates. Keep the original until the
        // replacement's confirmation AND routing are known; preserve logical identity.
        var body = envelope.Body;
        int attempts = 0;
        bool blocked = false;
        try
        {
            while (CanSettle(source, epoch))
            {
                cancellationToken.ThrowIfCancellationRequested();
                try
                {
                    using var attempt = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                    attempt.CancelAfter(TimeSpan.FromSeconds(10));
                    await PublishHandoffAsync(exchange, routingKey, properties, body, attempt.Token).AnyContext();
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception exception) when (exception is not OutOfMemoryException and not StackOverflowException)
                {
                    _deliveryError = exception;
                    if (!blocked)
                    {
                        Interlocked.Increment(ref _blockedDeliveries);
                        blocked = true;
                    }
                    attempts = Math.Min(attempts + 1, 10);
                    _logger.LogWarning(exception, "Retry/terminal handoff failed or is ambiguous; retaining original delivery (backoff {Seconds}s)", attempts);
                    await Task.Delay(TimeSpan.FromSeconds(attempts), cancellationToken).AnyContext();
                    continue;
                }
                // ACK failure must not itself republish another replacement from this callback.
                if (CanSettle(source, epoch))
                    await source.BasicAckAsync(envelope.DeliveryTag, false, cancellationToken).AnyContext();
                _deliveryError = null;
                return;
            }
        }
        finally
        {
            if (blocked)
                Interlocked.Decrement(ref _blockedDeliveries);
        }
    }

    /// <summary>
    /// Publish a replacement on a dedicated confirmed channel with mandatory routing.
    /// Completion means broker confirmation without a return, not downstream processing.
    /// </summary>
    protected virtual async Task PublishHandoffAsync(string exchange, string routingKey, BasicProperties properties,
        ReadOnlyMemory<byte> body, CancellationToken cancellationToken)
    {
        using (await _handoffLock.LockAsync(cancellationToken).AnyContext())
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                if (_handoffConnection is not { IsOpen: true })
                {
                    await ClearHandoffTransportAsync().AnyContext();
                    _handoffConnection = await CreateConnectionAsync(cancellationToken).AnyContext();
                }
                if (_handoffChannel is not { IsOpen: true })
                {
                    await DisposeTransportAsync(_handoffChannel, "handoff channel").AnyContext();
                    _handoffChannel = await _handoffConnection.CreateChannelAsync(
                        new CreateChannelOptions(publisherConfirmationsEnabled: true, publisherConfirmationTrackingEnabled: true),
                        cancellationToken).AnyContext();
                }
                await _handoffChannel.BasicPublishAsync(exchange, routingKey, mandatory: true, properties, body, cancellationToken).AnyContext();
            }
            catch
            {
                await ClearHandoffTransportAsync().AnyContext();
                throw;
            }
        }
    }

    protected override object? DeserializeMessageBody(IMessage message)
    {
        if (!_options.RequireSuccessfulDispatch)
            return base.DeserializeMessageBody(message);
        try
        {
            var type = message.ClrType ?? GetMappedMessageType(message.Type)
                ?? throw new InvalidDeliveryException("The required typed message schema could not be resolved.");
            if (message.Data.IsEmpty)
                throw new InvalidDeliveryException("The required typed message body is empty.");
            return _serializer.Deserialize(message.Data, type)
                ?? throw new InvalidDeliveryException("The required typed message body deserialized to null.");
        }
        catch (InvalidDeliveryException)
        {
            throw;
        }
        catch (Exception exception) when (exception is not OutOfMemoryException and not StackOverflowException)
        {
            throw new InvalidDeliveryException("The required typed message body could not be deserialized.", exception);
        }
    }

    private async Task<bool> DispatchRequiredAsync(IMessage message, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var subscribers = GetMessageSubscribers(message)
            .Where(subscriber => !subscriber.CancellationToken.IsCancellationRequested).ToArray();
        if (subscribers.Length == 0)
        {
            if (_subscribers.IsEmpty)
                return false;
            throw new InvalidDeliveryException("No matching live handler exists for a required delivery.");
        }
        object? body = null;
        if (subscribers.Any(subscriber => subscriber.Type != typeof(IMessage)))
            body = message.GetBody() ?? throw new InvalidDeliveryException("The required typed payload is null.");

        var handlers = subscribers.Select(subscriber => Task.Run(async () =>
        {
            using var lifetime = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, subscriber.CancellationToken);
            using var activity = StartHandleMessageActivity(message);
            try
            {
                lifetime.Token.ThrowIfCancellationRequested();
                object value = subscriber.Type == typeof(IMessage) ? message
                    : subscriber.GenericType is not null
                        ? Activator.CreateInstance(subscriber.GenericType, message)
                            ?? throw new InvalidDeliveryException("A required typed message wrapper could not be created.")
                        : body!;
                await subscriber.Action(value, lifetime.Token).WaitAsync(lifetime.Token).AnyContext();
                return !subscriber.CancellationToken.IsCancellationRequested;
            }
            catch (OperationCanceledException) when (subscriber.CancellationToken.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
            {
                // A removed subscription must not prevent other live handlers from progressing.
                return false;
            }
            catch (Exception exception)
            {
                activity?.SetErrorStatus(exception);
                throw;
            }
        }, cancellationToken)).ToArray();
        var completed = await Task.WhenAll(handlers).WaitAsync(cancellationToken).AnyContext();
        cancellationToken.ThrowIfCancellationRequested();
        return completed.Any(value => value);
    }

    private async Task PublishMessageAsync(string exchange, string routingKey, ReadOnlyMemory<byte> body,
        BasicProperties properties, CancellationToken cancellationToken)
    {
        using var lifetime = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _shutdownToken);
        var token = lifetime.Token;
        await _resiliencePolicy.ExecuteAsync(async _ =>
        {
            token.ThrowIfCancellationRequested();
            if (!_publisherReady.IsSet)
            {
                if (_options.PublishRecoveryTimeout <= TimeSpan.Zero)
                    throw new MessageBusException("Cannot publish: publisher channel is closed or unavailable.");
                try
                {
                    await _publisherReady.WaitAsync(token).WaitAsync(_options.PublishRecoveryTimeout, token).AnyContext();
                }
                catch (TimeoutException)
                {
                    throw new MessageBusException(FormattableString.Invariant($"Publish failed: connection recovery did not complete within {_options.PublishRecoveryTimeout.TotalMilliseconds:F0}ms timeout."));
                }
            }
            using (await _lock.LockAsync(token).AnyContext())
            {
                token.ThrowIfCancellationRequested();
                if (_publisherChannel is not { IsOpen: true } channel)
                    throw new MessageBusException("Cannot publish: publisher channel is closed or unavailable.");
                if (_isPublisherBlocked)
                    throw new MessageBusException($"Cannot publish: publisher connection is blocked by broker ({_publisherBlockedReason ?? "resource alarm"})");
                await channel.BasicPublishAsync(exchange, routingKey, _options.RequirePublishRouting, properties, body, token).AnyContext();
            }
        }, token).AnyContext();
    }

    protected virtual IMessage ConvertToMessage(BasicDeliverEventArgs envelope) =>
        RabbitMQMessageConverter.Convert(envelope, GetMappedMessageType(envelope.BasicProperties.Type), _deserializeMessageBody);

    protected override async Task EnsureTopicCreatedAsync(CancellationToken cancellationToken)
    {
        // A closed channel on a live connection needs repair. During network recovery,
        // however, the client's recovery owns the existing channel and the publish gate.
        if (_publisherChannel is { IsOpen: true } || (_publisherConnection is not null && !_publisherReady.IsSet))
            return;

        using var setup = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _shutdownToken);
        setup.CancelAfter(TimeSpan.FromSeconds(30));
        using (await _lock.LockAsync(setup.Token).AnyContext())
        {
            if (_publisherChannel is { IsOpen: true } || (_publisherConnection is not null && !_publisherReady.IsSet))
                return;
            setup.Token.ThrowIfCancellationRequested();
            if (IsDisposed)
                throw new MessageBusException("Cannot initialize a disposed message bus.");

            try
            {
                await DisposeTransportAsync(_publisherChannel, "publisher channel").AnyContext();
                _publisherChannel = null;
                if (_publisherConnection is not { IsOpen: true })
                {
                    await ClearPublisherTransportAsync().AnyContext();
                    _publisherConnection = await CreateConnectionAsync(setup.Token).AnyContext();
                    RegisterPublisherConnectionEventHandlers();
                }
                var serverVersion = _topology.DetectServerVersion(_publisherConnection);
                _publisherChannel = await CreatePublisherChannelAsync(setup.Token).AnyContext();
                var delayed = await _topology.CreateDelayedExchangeAsync(_publisherChannel, serverVersion, setup.Token).AnyContext();
                if (delayed is false)
                {
                    // An unknown exchange type may close the connection, not just the channel.
                    await ClearPublisherTransportAsync().AnyContext();
                    _publisherConnection = await CreateConnectionAsync(setup.Token).AnyContext();
                    RegisterPublisherConnectionEventHandlers();
                    _publisherChannel = await CreatePublisherChannelAsync(setup.Token).AnyContext();
                }
                if (delayed is not true)
                    await _topology.CreateRegularExchangeAsync(_publisherChannel, setup.Token).AnyContext();
                // Recreating a channel on an existing connection must not erase a broker alarm.
                _publisherReady.Set();
            }
            catch
            {
                await ClearPublisherTransportAsync().AnyContext();
                _publisherReady.Set();
                throw;
            }
        }
    }

    private Task OnPublisherConnectionOnCallbackExceptionAsync(object sender, CallbackExceptionEventArgs e)
    {
        _logger.LogError(e.Exception, "Publisher callback exception");
        return Task.CompletedTask;
    }

    private Task OnPublisherConnectionOnConnectionBlockedAsync(object sender, ConnectionBlockedEventArgs e)
    {
        _publisherBlockedReason = e.Reason;
        _isPublisherBlocked = true;
        _logger.LogWarning("Publisher blocked by broker: {Reason}", e.Reason);
        return Task.CompletedTask;
    }

    private Task OnPublisherConnectionOnConnectionRecoveryErrorAsync(object sender, ConnectionRecoveryErrorEventArgs e)
    {
        _logger.LogError(e.Exception, "Publisher connection recovery failed");
        return Task.CompletedTask;
    }

    private Task OnPublisherConnectionOnConnectionShutdownAsync(object sender, ShutdownEventArgs e)
    {
        if (e.Initiator != ShutdownInitiator.Application)
            _publisherReady.Reset();
        _logger.LogInformation("Publisher shutdown: {ReplyCode} {ReplyText}", e.ReplyCode, e.ReplyText);
        return Task.CompletedTask;
    }

    private Task OnPublisherConnectionOnConnectionUnblockedAsync(object sender, AsyncEventArgs e)
    {
        _isPublisherBlocked = false;
        _publisherBlockedReason = null;
        return Task.CompletedTask;
    }

    private Task OnPublisherConnectionOnRecoverySucceededAsync(object sender, AsyncEventArgs e)
    {
        _isPublisherBlocked = false;
        _publisherBlockedReason = null;
        _publisherReady.Set();
        _logger.LogInformation("Publisher connection recovered");
        return Task.CompletedTask;
    }

    protected override async Task PublishImplAsync(string messageType, object message, MessageOptions options, CancellationToken cancellationToken)
    {
        byte[] data = SerializeMessageBody(messageType, message);
        bool delayed = options.DeliveryDelay.GetValueOrDefault() > TimeSpan.Zero;
        if (delayed && _options.RequirePublishRouting)
            throw new MessageBusException("Immediate routing confirmation is not supported for scheduled publications. Use a durable outbox or a separately verified scheduler.");
        if (delayed && !_topology.SupportsDelayedExchange)
        {
            if (_options.RequireBrokerDelayedDelivery)
                throw new MessageBusException("Broker-side delayed delivery is required but unavailable; no in-memory message was scheduled.");
            var mappedType = GetMappedMessageType(messageType)
                ?? throw new MessageBusException($"Unable to resolve CLR type for delayed message: {messageType}");
            _logger.LogWarning("Scheduling a best-effort delayed message in process memory; it will not survive process termination ({MessageType})", messageType);
            SendDelayedMessage(mappedType, message, options);
            return;
        }

        var properties = RabbitMQMessageConverter.CreateProperties(messageType, options, _options);
        await PublishMessageAsync(_options.Topic, String.Empty, data, properties, cancellationToken).AnyContext();
    }

    private Task<IConnection> CreateConnectionAsync(CancellationToken cancellationToken = default) =>
        _factory.CreateConnectionAsync(_endpoints, cancellationToken: cancellationToken);

    private Task<IChannel> CreatePublisherChannelAsync(CancellationToken cancellationToken)
    {
        if (_publisherConnection is null)
            throw new MessageBusException("Publisher connection must be initialized before creating a channel.");
        var options = _options.PublisherConfirmsEnabled || _options.RequirePublishRouting
            ? new CreateChannelOptions(publisherConfirmationsEnabled: true, publisherConfirmationTrackingEnabled: true)
            : null;
        return _publisherConnection.CreateChannelAsync(options, cancellationToken);
    }

    /// <summary>Parse the broker version from the UTF-8 AMQP server property.</summary>
    public static Version? ParseServerVersion(IDictionary<string, object?>? serverProperties) =>
        RabbitMQTopology.ParseServerVersion(serverProperties);

    private async Task ClearPublisherTransportAsync()
    {
        var channel = _publisherChannel;
        _publisherChannel = null;
        UnregisterPublisherConnectionEventHandlers();
        var connection = _publisherConnection;
        _publisherConnection = null;
        await DisposeTransportAsync(channel, "publisher channel").AnyContext();
        await DisposeTransportAsync(connection, "publisher connection").AnyContext();
        _isPublisherBlocked = false;
        _publisherBlockedReason = null;
    }

    private async Task ClearSubscriberChannelAsync()
    {
        if (_subscriberChannel is null && _consumer is null)
            return;
        InvalidateDeliveries();
        if (_consumer is not null)
        {
            _consumer.ReceivedAsync -= OnMessageAsync;
            _consumer.ShutdownAsync -= OnConsumerShutdownAsync;
            _consumer.UnregisteredAsync -= OnConsumerUnregisteredAsync;
            _consumer = null;
        }
        var channel = _subscriberChannel;
        _subscriberChannel = null;
        if (channel is not null)
            channel.ChannelShutdownAsync -= OnSubscriberChannelShutdownAsync;
        await DisposeTransportAsync(channel, "subscriber channel").AnyContext();
    }

    private async Task ClearSubscriberConnectionAsync()
    {
        UnregisterSubscriberConnectionEventHandlers();
        var connection = _subscriberConnection;
        _subscriberConnection = null;
        _subscriberRecovering = false;
        await DisposeTransportAsync(connection, "subscriber connection").AnyContext();
    }

    private async Task ClearHandoffTransportAsync()
    {
        var channel = _handoffChannel;
        var connection = _handoffConnection;
        _handoffChannel = null;
        _handoffConnection = null;
        await DisposeTransportAsync(channel, "handoff channel").AnyContext();
        await DisposeTransportAsync(connection, "handoff connection").AnyContext();
    }

    private async Task DisposeTransportAsync(IAsyncDisposable? resource, string role)
    {
        if (resource is null)
            return;
        try
        {
            await resource.DisposeAsync().AsTask().WaitAsync(_options.ShutdownTimeout).AnyContext();
        }
        catch (Exception exception) when (exception is not OutOfMemoryException and not StackOverflowException)
        {
            _logger.LogWarning(exception, "Transport cleanup failed or exceeded its timeout ({Role})", role);
        }
    }

    private void RegisterPublisherConnectionEventHandlers()
    {
        if (_publisherConnection is null)
            throw new MessageBusException("Publisher connection has not been initialized.");
        _publisherConnection.CallbackExceptionAsync += OnPublisherConnectionOnCallbackExceptionAsync;
        _publisherConnection.ConnectionBlockedAsync += OnPublisherConnectionOnConnectionBlockedAsync;
        _publisherConnection.ConnectionRecoveryErrorAsync += OnPublisherConnectionOnConnectionRecoveryErrorAsync;
        _publisherConnection.ConnectionShutdownAsync += OnPublisherConnectionOnConnectionShutdownAsync;
        _publisherConnection.ConnectionUnblockedAsync += OnPublisherConnectionOnConnectionUnblockedAsync;
        _publisherConnection.RecoverySucceededAsync += OnPublisherConnectionOnRecoverySucceededAsync;
    }

    private void UnregisterPublisherConnectionEventHandlers()
    {
        if (_publisherConnection is null)
            return;
        _publisherConnection.CallbackExceptionAsync -= OnPublisherConnectionOnCallbackExceptionAsync;
        _publisherConnection.ConnectionBlockedAsync -= OnPublisherConnectionOnConnectionBlockedAsync;
        _publisherConnection.ConnectionRecoveryErrorAsync -= OnPublisherConnectionOnConnectionRecoveryErrorAsync;
        _publisherConnection.ConnectionShutdownAsync -= OnPublisherConnectionOnConnectionShutdownAsync;
        _publisherConnection.ConnectionUnblockedAsync -= OnPublisherConnectionOnConnectionUnblockedAsync;
        _publisherConnection.RecoverySucceededAsync -= OnPublisherConnectionOnRecoverySucceededAsync;
    }

    private void RegisterSubscriberConnectionEventHandlers()
    {
        if (_subscriberConnection is null)
            throw new MessageBusException("Subscriber connection has not been initialized.");
        _subscriberConnection.ConnectionShutdownAsync += OnSubscriberConnectionOnConnectionShutdownAsync;
        _subscriberConnection.RecoverySucceededAsync += OnSubscriberConnectionOnRecoverySucceededAsync;
        _subscriberConnection.ConnectionRecoveryErrorAsync += OnSubscriberConnectionOnConnectionRecoveryErrorAsync;
        _subscriberConnection.CallbackExceptionAsync += OnSubscriberConnectionOnCallbackExceptionAsync;
        _subscriberConnection.QueueNameChangedAfterRecoveryAsync += OnQueueNameChangedAfterRecoveryAsync;
    }

    private void UnregisterSubscriberConnectionEventHandlers()
    {
        if (_subscriberConnection is null)
            return;
        _subscriberConnection.ConnectionShutdownAsync -= OnSubscriberConnectionOnConnectionShutdownAsync;
        _subscriberConnection.RecoverySucceededAsync -= OnSubscriberConnectionOnRecoverySucceededAsync;
        _subscriberConnection.ConnectionRecoveryErrorAsync -= OnSubscriberConnectionOnConnectionRecoveryErrorAsync;
        _subscriberConnection.CallbackExceptionAsync -= OnSubscriberConnectionOnCallbackExceptionAsync;
        _subscriberConnection.QueueNameChangedAfterRecoveryAsync -= OnQueueNameChangedAfterRecoveryAsync;
    }

    /// <summary>Close the recovery gate for deterministic gate tests.</summary>
    internal void SimulatePublisherConnectionLost() => _publisherReady.Reset();

    /// <summary>Open the recovery gate for deterministic gate tests.</summary>
    internal void SimulatePublisherRecoverySucceeded() => _publisherReady.Set();

    /// <summary>Simulate network loss, not a channel-only failure.</summary>
    internal async Task SimulatePublisherConnectionShutdownAsync()
    {
        await OnPublisherConnectionOnConnectionShutdownAsync(this,
            new ShutdownEventArgs(ShutdownInitiator.Library, 541, "Simulated connection reset")).AnyContext();
        _publisherChannel = null;
    }

    /// <summary>Verify a failed recovery attempt does not open the gate.</summary>
    internal Task SimulatePublisherConnectionRecoveryErrorAsync() =>
        OnPublisherConnectionOnConnectionRecoveryErrorAsync(this,
            new ConnectionRecoveryErrorEventArgs(new Exception("Simulated recovery failure")));

    private sealed class DeliveryEpoch
    {
        private readonly CancellationTokenSource _source;
        private int _cancelled;
        internal CancellationToken Token { get; }
        internal DeliveryEpoch(CancellationToken shutdown)
        {
            _source = CancellationTokenSource.CreateLinkedTokenSource(shutdown);
            Token = _source.Token;
        }

        internal void Cancel(ILogger logger)
        {
            if (Interlocked.Exchange(ref _cancelled, 1) == 0)
                _ = CancelAndDisposeAsync(logger);
        }

        private async Task CancelAndDisposeAsync(ILogger logger)
        {
            try
            {
                // CancelAsync marks the token immediately but invokes callbacks off the
                // transport event path. A slow handler cancellation cannot block recovery.
                await _source.CancelAsync().AnyContext();
            }
            catch (Exception exception) when (exception is not OutOfMemoryException and not StackOverflowException)
            {
                logger.LogWarning(exception, "A delivery cancellation callback failed");
            }
            finally
            {
                _source.Dispose();
            }
        }
    }

    private sealed class InvalidDeliveryException : MessageBusException
    {
        internal InvalidDeliveryException(string message) : base(message) { }
        internal InvalidDeliveryException(string message, Exception inner) : base(message, inner) { }
    }
}
