using Xunit;

namespace Foundatio.RabbitMQ.Tests;

[CollectionDefinition(nameof(RabbitMqTestCollection), DisableParallelization = true)]
public sealed class RabbitMqTestCollection : ICollectionFixture<AspireFixture>;
