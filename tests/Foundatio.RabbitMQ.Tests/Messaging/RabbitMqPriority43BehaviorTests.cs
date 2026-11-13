using System.Threading.Tasks;
using Xunit;

namespace Foundatio.RabbitMQ.Tests.Messaging;

[Collection(nameof(RabbitMqTestCollection))]
public class RabbitMqPriority43BehaviorTests(AspireFixture fixture, ITestOutputHelper output)
    : RabbitMqPriorityBehaviorTestBase(fixture.MessagingPriority43ConnectionString!, output)
{
    [Fact]
    public async Task Fixture_WithUpgradeBroker_UsesRabbitMq43()
    {
        Assert.SkipWhen(!fixture.IsAvailable, "RabbitMQ infrastructure not available");

        // Act
        var version = await GetBrokerVersionAsync();

        // Assert
        Assert.Equal(4, version.Major);
        Assert.Equal(3, version.Minor);
    }
}
