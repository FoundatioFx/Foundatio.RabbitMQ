using Xunit;

namespace Foundatio.RabbitMQ.Tests.Messaging;

[Collection(nameof(RabbitMqTestCollection))]
public class RabbitMqMessageBusClassicTests(AspireFixture fixture, ITestOutputHelper output)
    : RabbitMqMessageBusClassicTestBase(fixture.MessagingConnectionString!, output);
