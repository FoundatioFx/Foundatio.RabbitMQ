using Xunit;

namespace Foundatio.RabbitMQ.Tests.Messaging;

[Collection("Aspire")]
public class RabbitMqMessageBusClassicTests : RabbitMqMessageBusClassicTestBase
{
    public RabbitMqMessageBusClassicTests(AspireFixture fixture, ITestOutputHelper output)
        : base(fixture.MessagingConnectionString, output) { }
}
