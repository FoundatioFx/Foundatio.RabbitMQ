using Xunit;

namespace Foundatio.RabbitMQ.Tests.Messaging;

[Collection(nameof(RabbitMqTestCollection))]
public class RabbitMqMessageBusClassicDelayedExchangeTests : RabbitMqMessageBusClassicTestBase
{
    public RabbitMqMessageBusClassicDelayedExchangeTests(AspireFixture fixture, ITestOutputHelper output)
        : base(fixture.MessagingDelayedConnectionString ?? fixture.MessagingConnectionString!, output)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(fixture.MessagingDelayedConnectionString),
            "Delayed exchange container not available");
    }
}
