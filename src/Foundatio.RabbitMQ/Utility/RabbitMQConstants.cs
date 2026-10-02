namespace Foundatio.Utility;

/// <summary>RabbitMQ wire header and queue argument names shared by publishers, consumers, and topology setup.</summary>
public static class RabbitMQConstants
{
    /// <summary>The <c>x-delivery-count</c> wire name.</summary>
    public const string XDeliveryCountHeader = "x-delivery-count";

    /// <summary>The <c>x-original-message-id</c> wire name.</summary>
    public const string XOriginalMessageIdHeader = "x-original-message-id";

    /// <summary>The <c>x-delay</c> wire name.</summary>
    public const string XDelayHeader = "x-delay";

    /// <summary>The <c>x-original-expiration</c> wire name.</summary>
    public const string XOriginalExpirationHeader = "x-original-expiration";

    /// <summary>The <c>x-foundatio-failure-type</c> wire name.</summary>
    public const string FailureTypeHeader = "x-foundatio-failure-type";

    /// <summary>The <c>x-foundatio-original-exchange</c> wire name.</summary>
    public const string OriginalExchangeHeader = "x-foundatio-original-exchange";

    /// <summary>The <c>x-foundatio-original-routing-key</c> wire name.</summary>
    public const string OriginalRoutingKeyHeader = "x-foundatio-original-routing-key";

    /// <summary>The <c>CC</c> wire name.</summary>
    public const string CarbonCopyHeader = "CC";

    /// <summary>The <c>BCC</c> wire name.</summary>
    public const string BlindCarbonCopyHeader = "BCC";

    /// <summary>The <c>x-queue-type</c> wire name.</summary>
    public const string QueueTypeArgument = "x-queue-type";

    /// <summary>The <c>x-delivery-limit</c> wire name.</summary>
    public const string DeliveryLimitArgument = "x-delivery-limit";

    /// <summary>The <c>x-dead-letter-exchange</c> wire name.</summary>
    public const string DeadLetterExchangeArgument = "x-dead-letter-exchange";

    /// <summary>The <c>x-dead-letter-routing-key</c> wire name.</summary>
    public const string DeadLetterRoutingKeyArgument = "x-dead-letter-routing-key";

    /// <summary>The <c>x-dead-letter-strategy</c> wire name.</summary>
    public const string DeadLetterStrategyArgument = "x-dead-letter-strategy";

    /// <summary>The <c>x-overflow</c> wire name.</summary>
    public const string OverflowArgument = "x-overflow";

    /// <summary>The <c>x-consumer-timeout</c> wire name.</summary>
    public const string ConsumerTimeoutArgument = "x-consumer-timeout";

    /// <summary>The <c>x-single-active-consumer</c> wire name.</summary>
    public const string SingleActiveConsumerArgument = "x-single-active-consumer";

    /// <summary>The <c>x-max-priority</c> wire name.</summary>
    public const string MaxPriorityArgument = "x-max-priority";

    /// <summary>The <c>x-delayed-retry-type</c> wire name.</summary>
    public const string DelayedRetryTypeArgument = "x-delayed-retry-type";

    /// <summary>The <c>x-delayed-retry-min</c> wire name.</summary>
    public const string DelayedRetryMinArgument = "x-delayed-retry-min";

    /// <summary>The <c>x-delayed-retry-max</c> wire name.</summary>
    public const string DelayedRetryMaxArgument = "x-delayed-retry-max";

    /// <summary>The <c>x-delayed-type</c> wire name.</summary>
    public const string DelayedExchangeTypeArgument = "x-delayed-type";

    /// <summary>The <c>x-max-length-bytes</c> wire name.</summary>
    public const string MaxLengthBytesArgument = "x-max-length-bytes";
}
