using Xunit;

namespace Foundatio.RabbitMQ.Tests.Messaging;

public class RabbitMqPriorityBehaviorTests(AspireFixture fixture, ITestOutputHelper output)
    : RabbitMqPriorityBehaviorTestBase(fixture.MessagingConnectionString!, output), IClassFixture<AspireFixture>;
