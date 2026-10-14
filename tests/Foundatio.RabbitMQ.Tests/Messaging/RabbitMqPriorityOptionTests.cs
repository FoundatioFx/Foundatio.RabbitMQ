using System;
using System.Collections.Generic;
using Foundatio.Messaging;
using Xunit;

namespace Foundatio.RabbitMQ.Tests.Messaging;

public class RabbitMqPriorityOptionTests
{
    [Fact]
    public void Constructor_WithDirectQuorumMaxPriority_Throws()
    {
        // Arrange
        var options = new RabbitMQMessageBusOptions
        {
            ConnectionString = "amqp://localhost",
            MaxPriority = 3,
            Arguments = new Dictionary<string, object?> { ["x-queue-type"] = "quorum" }
        };

        // Act
        Action create = () => new RabbitMQMessageBus(options);

        // Assert
        var exception = Assert.Throws<InvalidOperationException>(create);
        Assert.Contains("classic", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Constructor_WithMutatedArguments_Throws()
    {
        // Arrange
        var arguments = new Dictionary<string, object?>();
        var options = new RabbitMQMessageBusOptions { ConnectionString = "amqp://localhost", Arguments = arguments, MaxPriority = 3 };
        arguments["x-queue-type"] = "quorum";

        // Act
        Action create = () => new RabbitMQMessageBus(options);

        // Assert
        Assert.Throws<InvalidOperationException>(create);
    }

    [Fact]
    public void MaxPriority_WithClassicQueue_AllowsBrokerSupportedValue()
    {
        // Arrange
        var options = new RabbitMQMessageBusOptions();

        // Act
        options.MaxPriority = byte.MaxValue;

        // Assert
        Assert.Equal(byte.MaxValue, options.MaxPriority);
    }

    [Fact]
    public void MaxPriority_WithZero_ThrowsWithoutChangingOptions()
    {
        // Arrange
        var options = new RabbitMQMessageBusOptions();

        // Act
        Action configure = () => options.MaxPriority = 0;

        // Assert
        Assert.Throws<ArgumentOutOfRangeException>(configure);
        Assert.Null(options.MaxPriority);
    }

    [Fact]
    public void UseMessagePriority_AfterUseQuorumQueues_Throws()
    {
        // Arrange
        var builder = new RabbitMQMessageBusOptionsBuilder().UseQuorumQueues();

        // Act
        Action configure = () => builder.UseMessagePriority();

        // Assert
        var exception = Assert.Throws<InvalidOperationException>(configure);
        Assert.Contains("classic", exception.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Null(builder.Build().MaxPriority);
    }

    [Fact]
    public void UseMessagePriority_WithClassicQueue_SetsMaximum()
    {
        // Arrange
        var builder = new RabbitMQMessageBusOptionsBuilder();

        // Act
        builder.UseMessagePriority(4);

        // Assert
        Assert.Equal((byte)4, builder.Build().MaxPriority);
    }

    [Fact]
    public void UseQuorumQueues_AfterUseMessagePriority_ThrowsWithoutChangingOptions()
    {
        // Arrange
        var builder = new RabbitMQMessageBusOptionsBuilder().UseMessagePriority();

        // Act
        Action configure = () => builder.UseQuorumQueues();

        // Assert
        var exception = Assert.Throws<InvalidOperationException>(configure);
        Assert.Contains("classic", exception.Message, StringComparison.OrdinalIgnoreCase);
        var options = builder.Build();
        Assert.Null(options.Arguments);
        Assert.True(options.IsSubscriptionQueueExclusive);
        Assert.True(options.SubscriptionQueueAutoDelete);
    }
}
