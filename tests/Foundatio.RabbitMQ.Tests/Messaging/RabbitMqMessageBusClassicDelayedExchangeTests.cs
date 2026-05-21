using Xunit;

namespace Foundatio.RabbitMQ.Tests.Messaging;

public class RabbitMqMessageBusClassicDelayedExchangeTests : RabbitMqMessageBusClassicTestBase, IClassFixture<AspireFixture>
{
    private readonly string _delayedConnectionString;

    public RabbitMqMessageBusClassicDelayedExchangeTests(AspireFixture fixture, ITestOutputHelper output)
        : base(string.IsNullOrEmpty(fixture.MessagingDelayedConnectionString)
            ? fixture.MessagingConnectionString
            : fixture.MessagingDelayedConnectionString, output)
    {
        _delayedConnectionString = fixture.MessagingDelayedConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(_delayedConnectionString),
            "Delayed exchange container not available");
    }
}
