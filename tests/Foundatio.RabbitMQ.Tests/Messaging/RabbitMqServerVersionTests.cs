using System;
using System.Collections.Generic;
using System.Text;
using Foundatio.Messaging;
using Xunit;

namespace Foundatio.RabbitMQ.Tests.Messaging;

public class RabbitMqServerVersionTests
{
    [Theory]
    [InlineData("3.13.7", 3, 13)]
    [InlineData("4.0.5", 4, 0)]
    [InlineData("4.2.0", 4, 2)]
    [InlineData("4.3.0", 4, 3)]
    [InlineData("4.3.1", 4, 3)]
    [InlineData("5.0.0", 5, 0)]
    public void ParseServerVersion_WithValidVersionBytes_ReturnsVersion(string versionString, int major, int minor)
    {
        var properties = new Dictionary<string, object?>
        {
            ["version"] = Encoding.UTF8.GetBytes(versionString)
        };

        var result = RabbitMQMessageBus.ParseServerVersion(properties);

        Assert.NotNull(result);
        Assert.Equal(major, result.Major);
        Assert.Equal(minor, result.Minor);
    }

    [Fact]
    public void ParseServerVersion_WithNullProperties_ReturnsNull()
    {
        var result = RabbitMQMessageBus.ParseServerVersion(null);
        Assert.Null(result);
    }

    [Fact]
    public void ParseServerVersion_WithMissingVersionKey_ReturnsNull()
    {
        var properties = new Dictionary<string, object?>
        {
            ["product"] = Encoding.UTF8.GetBytes("RabbitMQ")
        };

        var result = RabbitMQMessageBus.ParseServerVersion(properties);
        Assert.Null(result);
    }

    [Fact]
    public void ParseServerVersion_WithNonByteArrayValue_ReturnsNull()
    {
        var properties = new Dictionary<string, object?>
        {
            ["version"] = "4.2.0"
        };

        var result = RabbitMQMessageBus.ParseServerVersion(properties);
        Assert.Null(result);
    }

    [Fact]
    public void ParseServerVersion_WithInvalidVersionBytes_ReturnsNull()
    {
        var properties = new Dictionary<string, object?>
        {
            ["version"] = Encoding.UTF8.GetBytes("not-a-version")
        };

        var result = RabbitMQMessageBus.ParseServerVersion(properties);
        Assert.Null(result);
    }

    [Fact]
    public void VersionGating_Rmq42_IsBelow43()
    {
        var rmq42 = new Version(4, 2, 0);
        var rmq43 = new Version(4, 3);
        Assert.True(rmq42 < rmq43);
    }

    [Fact]
    public void VersionGating_Rmq43_IsAtOrAbove43()
    {
        var rmq43 = new Version(4, 3, 0);
        var threshold = new Version(4, 3);
        Assert.True(rmq43 >= threshold);
    }

    [Fact]
    public void VersionGating_Rmq50_IsAbove43()
    {
        var rmq50 = new Version(5, 0, 0);
        var threshold = new Version(4, 3);
        Assert.True(rmq50 >= threshold);
    }
}
