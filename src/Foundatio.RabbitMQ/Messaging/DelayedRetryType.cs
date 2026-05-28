using System;
using System.Reflection;
using System.Runtime.Serialization;

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
    [EnumMember(Value = "all")]
    All,

    /// <summary>
    /// Only messages that failed delivery (rejects with requeue=true) are delayed.
    /// </summary>
    [EnumMember(Value = "failed")]
    Failed,

    /// <summary>
    /// Delayed retry is explicitly disabled.
    /// </summary>
    [EnumMember(Value = "disabled")]
    Disabled
}

internal static class EnumExtensions
{
    public static string ToEnumString<T>(this T value) where T : struct, Enum
    {
        var member = typeof(T).GetField(value.ToString()!);
        var attribute = member?.GetCustomAttribute<EnumMemberAttribute>();
        return attribute?.Value ?? value.ToString()!;
    }
}
