using Xunit;

namespace Foundatio.RabbitMQ.Tests.Messaging;

[Collection(nameof(RabbitMqTestCollection))]
public class RabbitMqMessageBusTests(AspireFixture fixture, ITestOutputHelper output)
    : RabbitMqMessageBusTestBase(fixture.MessagingConnectionString!, output);
