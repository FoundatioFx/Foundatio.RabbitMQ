using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using Foundatio.Utility;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Foundatio.Messaging;

internal static class RabbitMQMessageConverter
{
    private const string PriorityPropertyKey = "Priority";

    internal static IMessage Convert(BasicDeliverEventArgs envelope, Type? type, Func<IMessage, object?> deserialize)
    {
        // A handler can outlive the transport callback after cancellation, so own the payload.
        var message = new Message(envelope.Body.ToArray(), deserialize)
        {
            Type = envelope.BasicProperties.Type,
            ClrType = type,
            CorrelationId = envelope.BasicProperties.CorrelationId,
            UniqueId = envelope.BasicProperties.MessageId
        };
        if (envelope.BasicProperties.Headers is not null)
            foreach (var header in envelope.BasicProperties.Headers)
            {
                if (header.Value is byte[] bytes)
                    message.Properties[header.Key] = Encoding.UTF8.GetString(bytes);
                else if (header.Value is not null && System.Convert.ToString(header.Value, CultureInfo.InvariantCulture) is { } value)
                    message.Properties[header.Key] = value;
            }
        return message;
    }

    internal static BasicProperties CreateProperties(string messageType, MessageOptions options, RabbitMQMessageBusOptions settings)
    {
        bool delayed = options.DeliveryDelay.GetValueOrDefault() > TimeSpan.Zero;
        var properties = new BasicProperties
        {
            MessageId = options.UniqueId ?? Guid.NewGuid().ToString("N"),
            CorrelationId = options.CorrelationId,
            Type = messageType,
            Persistent = settings.IsDurable
        };
        if (settings.DefaultMessageTimeToLive.HasValue)
            properties.Expiration = settings.DefaultMessageTimeToLive.Value.TotalMilliseconds.ToString(CultureInfo.InvariantCulture);
        if (options.Properties.TryGetValue(PriorityPropertyKey, out string? priorityValue) && Byte.TryParse(priorityValue, NumberStyles.Integer, CultureInfo.InvariantCulture, out byte priority))
            properties.Priority = priority;
        if (options.Properties.Count > 0)
        {
            properties.Headers = new Dictionary<string, object?>();
            foreach (var property in options.Properties)
                if (!String.Equals(property.Key, PriorityPropertyKey, StringComparison.Ordinal))
                    properties.Headers.Add(property.Key, property.Value);
        }
        if (delayed)
        {
            double delayMs = options.DeliveryDelay!.Value.TotalMilliseconds;
            if (delayMs > Int32.MaxValue)
                throw new ArgumentOutOfRangeException(nameof(options), "DeliveryDelay exceeds the supported delayed-exchange range.");
            properties.Headers ??= new Dictionary<string, object?>();
            properties.Headers[RabbitMQConstants.XDelayHeader] = (int)delayMs;
        }
        return properties;
    }

    internal static BasicProperties CopyHandoffProperties(BasicDeliverEventArgs envelope)
    {
        var properties = new BasicProperties(envelope.BasicProperties)
        {
            UserId = null,
            Headers = envelope.BasicProperties.Headers is { } headers
                ? new Dictionary<string, object?>(headers) : new Dictionary<string, object?>()
        };
        properties.Headers.Remove(RabbitMQConstants.CarbonCopyHeader);
        properties.Headers.Remove(RabbitMQConstants.BlindCarbonCopyHeader);
        properties.Headers.Remove(RabbitMQConstants.XDelayHeader);
        if (!properties.Headers.ContainsKey(RabbitMQConstants.XOriginalMessageIdHeader))
            properties.Headers[RabbitMQConstants.XOriginalMessageIdHeader] = properties.MessageId;
        return properties;
    }

    internal static long GetRetryCountFromHeader(BasicDeliverEventArgs envelope)
    {
        if (envelope.BasicProperties.Headers?.TryGetValue(RabbitMQConstants.XDeliveryCountHeader, out object? value) is not true)
            return 0;
        if (value is long number)
            return number >= 0 ? number : Int64.MaxValue;
        string? text = value is byte[] bytes ? Encoding.UTF8.GetString(bytes) : System.Convert.ToString(value, CultureInfo.InvariantCulture);
        // Invalid/overflowing metadata must not reset an exhausted budget.
        return Int64.TryParse(text, NumberStyles.None, CultureInfo.InvariantCulture, out long count) && count >= 0
            ? count : Int64.MaxValue;
    }

}
