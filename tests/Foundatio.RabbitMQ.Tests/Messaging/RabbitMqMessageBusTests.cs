using Xunit;

namespace Foundatio.RabbitMQ.Tests.Messaging;

[Collection("Aspire")]
public class RabbitMqMessageBusTests : RabbitMqMessageBusTestBase
{
    public RabbitMqMessageBusTests(AspireFixture fixture, ITestOutputHelper output)
        : base(fixture.MessagingConnectionString, output) { }
}
