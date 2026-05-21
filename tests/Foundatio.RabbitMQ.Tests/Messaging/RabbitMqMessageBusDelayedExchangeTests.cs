using Xunit;

namespace Foundatio.RabbitMQ.Tests.Messaging;

public class RabbitMqMessageBusDelayedExchangeTests : RabbitMqMessageBusTestBase, IClassFixture<AspireFixture>
{
    private readonly string _delayedConnectionString;

    public RabbitMqMessageBusDelayedExchangeTests(AspireFixture fixture, ITestOutputHelper output)
        : base(string.IsNullOrEmpty(fixture.MessagingDelayedConnectionString)
            ? fixture.MessagingConnectionString
            : fixture.MessagingDelayedConnectionString, output)
    {
        _delayedConnectionString = fixture.MessagingDelayedConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(_delayedConnectionString),
            "Delayed exchange container not available");
    }
}
