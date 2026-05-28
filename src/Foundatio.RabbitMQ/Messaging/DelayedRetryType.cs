using System;

namespace Foundatio.Messaging;

/// <summary>
/// Specifies the native delayed retry behavior for quorum queues (RabbitMQ 4.3+).
/// Maps to the x-delayed-retry-type queue argument.
/// See: https://www.rabbitmq.com/docs/quorum-queues#delayed-retries
/// </summary>
public enum DelayedRetryType
{
    /// <summary>
    /// All returned messages (nacks, rejects, and timeouts) are delayed before redelivery.
    /// </summary>
    All,

    /// <summary>
    /// Only messages that failed delivery (rejects with requeue=true) are delayed.
    /// </summary>
    Failed,

    /// <summary>
    /// Delayed retry is explicitly disabled.
    /// </summary>
    Disabled
}

internal static class DelayedRetryTypeExtensions
{
    public static string ToRabbitMQString(this DelayedRetryType type) => type switch
    {
        DelayedRetryType.All => "all",
        DelayedRetryType.Failed => "failed",
        DelayedRetryType.Disabled => "disabled",
        _ => throw new ArgumentOutOfRangeException(nameof(type), type, null)
    };
}
