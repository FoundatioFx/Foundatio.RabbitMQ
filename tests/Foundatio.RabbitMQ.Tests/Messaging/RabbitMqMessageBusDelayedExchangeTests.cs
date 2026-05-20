using Xunit;

namespace Foundatio.RabbitMQ.Tests.Messaging;

[Collection("Aspire")]
public class RabbitMqMessageBusDelayedExchangeTests : RabbitMqMessageBusTestBase
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
