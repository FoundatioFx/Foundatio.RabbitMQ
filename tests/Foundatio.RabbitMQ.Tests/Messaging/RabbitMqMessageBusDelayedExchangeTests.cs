using Xunit;

namespace Foundatio.RabbitMQ.Tests.Messaging;

[Collection(nameof(RabbitMqTestCollection))]
public class RabbitMqMessageBusDelayedExchangeTests : RabbitMqMessageBusTestBase
{
    public RabbitMqMessageBusDelayedExchangeTests(AspireFixture fixture, ITestOutputHelper output)
        : base(fixture.MessagingDelayedConnectionString ?? fixture.MessagingConnectionString!, output)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(fixture.MessagingDelayedConnectionString),
            "Delayed exchange container not available");
    }
}
