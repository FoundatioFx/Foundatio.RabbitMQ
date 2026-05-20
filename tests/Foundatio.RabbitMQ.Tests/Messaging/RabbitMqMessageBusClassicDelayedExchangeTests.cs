using Xunit;

namespace Foundatio.RabbitMQ.Tests.Messaging;

[Collection("Aspire")]
public class RabbitMqMessageBusClassicDelayedExchangeTests : RabbitMqMessageBusClassicTestBase
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
