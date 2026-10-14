using Xunit;

namespace Foundatio.RabbitMQ.Tests.Messaging;

[Collection(nameof(RabbitMqTestCollection))]
public class RabbitMqPriorityBehaviorTests(AspireFixture fixture, ITestOutputHelper output)
    : RabbitMqPriorityBehaviorTestBase(fixture.MessagingConnectionString!, output);
