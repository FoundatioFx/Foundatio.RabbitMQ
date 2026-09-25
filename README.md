![Foundatio](https://raw.githubusercontent.com/FoundatioFx/Foundatio/master/media/foundatio-dark-bg.svg#gh-dark-mode-only "Foundatio")![Foundatio](https://raw.githubusercontent.com/FoundatioFx/Foundatio/master/media/foundatio.svg#gh-light-mode-only "Foundatio")

[![Build status](https://github.com/FoundatioFx/Foundatio.RabbitMQ/workflows/Build/badge.svg)](https://github.com/FoundatioFx/Foundatio.RabbitMQ/actions)
[![NuGet Version](http://img.shields.io/nuget/v/Foundatio.RabbitMQ.svg?style=flat)](https://www.nuget.org/packages/Foundatio.RabbitMQ/)
[![feedz.io](https://img.shields.io/badge/endpoint.svg?url=https%3A%2F%2Ff.feedz.io%2Ffoundatio%2Ffoundatio%2Fshield%2FFoundatio.RabbitMQ%2Flatest)](https://f.feedz.io/foundatio/foundatio/packages/Foundatio.RabbitMQ/latest/download)
[![Discord](https://img.shields.io/discord/715744504891703319)](https://discord.gg/6HxgFCx)

# Foundatio.RabbitMQ

RabbitMQ pub/sub messaging for Foundatio's `IMessageBus` abstraction. This repository contains the provider implementation, XML API comments, samples, and tests. **Provider documentation is maintained in [FoundatioFx/Foundatio](https://github.com/FoundatioFx/Foundatio/tree/main/docs).**

## ✨ Why Choose Foundatio?

- 🔌 **Pluggable implementations** - Swap Redis, Azure, AWS, or in-memory with no code changes
- 🧪 **Developer friendly** - In-memory implementations for fast local development and testing
- 💉 **DI native** - Built for Microsoft.Extensions.DependencyInjection
- 🎯 **Interface-first** - Code against abstractions, not implementations
- ⚡ **Production ready** - Battle-tested in high-scale applications
- 🔄 **Consistent APIs** - Same patterns across caching, queues, storage, and more

## 🧱 Core Building Blocks

| Feature | Description |
|---------|-------------|
| [**Caching**](https://foundatio.dev/guide/caching) | In-memory, Redis, and hybrid caching with automatic invalidation |
| [**Queues**](https://foundatio.dev/guide/queues) | Reliable message queuing with Redis, Azure, AWS SQS |
| [**Locks**](https://foundatio.dev/guide/locks) | Distributed locking and throttling |
| [**Messaging**](https://foundatio.dev/guide/messaging) | Pub/sub with Redis, RabbitMQ, Kafka, Azure Service Bus |
| [**Jobs**](https://foundatio.dev/guide/jobs) | Background job processing with queue integration |
| [**File Storage**](https://foundatio.dev/guide/storage) | Unified file API for disk, S3, Azure Blob, and more |
| [**Resilience**](https://foundatio.dev/guide/resilience) | Retry policies, circuit breakers, and timeouts |

## 🚀 Quick Start

```bash
dotnet add package Foundatio.RabbitMQ
```

```csharp
using Foundatio.Messaging;

await using var messageBus = new RabbitMQMessageBus(o => o
    .ConnectionString("amqp://localhost")
    .Topic("events"));

await messageBus.SubscribeAsync<MyMessage>(message =>
{
    Console.WriteLine(message.Data);
});

await messageBus.PublishAsync(new MyMessage { Data = "Hello" });
```

`MyMessage` is your application message type. This local plaintext example uses best-effort defaults, not a required-delivery profile. Keep the bus alive for the subscription's intended lifetime; publication completion does not mean a handler has completed. Use `amqps` for encrypted transport.

## 📦 Provider Implementations

| Provider | Caching | Queues | Messaging | Storage | Locks |
|----------|---------|--------|-----------|---------|-------|
| [In-Memory](https://foundatio.dev/guide/implementations/in-memory) | ✅ | ✅ | ✅ | ✅ | ✅ |
| [Redis](https://github.com/FoundatioFx/Foundatio.Redis) | ✅ | ✅ | ✅ | ✅ | ✅ |
| [Azure Storage](https://github.com/FoundatioFx/Foundatio.AzureStorage) | | ✅ | | ✅ | |
| [Azure Service Bus](https://github.com/FoundatioFx/Foundatio.AzureServiceBus) | | ✅ | ✅ | | |
| [AWS (S3/SQS/SNS)](https://github.com/FoundatioFx/Foundatio.AWS) | | ✅ | ✅ | ✅ | |
| [RabbitMQ](https://github.com/FoundatioFx/Foundatio.RabbitMQ) | | | ✅ | | |
| [Kafka](https://github.com/FoundatioFx/Foundatio.Kafka) | | | ✅ | | |
| [Minio](https://github.com/FoundatioFx/Foundatio.Minio) | | | | ✅ | |
| [Aliyun](https://github.com/FoundatioFx/Foundatio.Aliyun) | | | | ✅ | |
| [SFTP](https://github.com/FoundatioFx/Foundatio.Storage.SshNet) | | | | ✅ | |

## 📚 Learn More

- [Published RabbitMQ overview](https://foundatio.dev/guide/implementations/rabbitmq)
- [Companion documentation PR #574](https://github.com/FoundatioFx/Foundatio/pull/574) for the implementation in [provider PR #100](https://github.com/FoundatioFx/Foundatio.RabbitMQ/pull/100)
- [Branch guide: configuration and TLS](https://github.com/FoundatioFx/Foundatio/blob/docs/rabbitmq-4.2.5-delivery-contracts/docs/guide/implementations/rabbitmq.md)
- [Branch guide: delivery safety and adoption](https://github.com/FoundatioFx/Foundatio/blob/docs/rabbitmq-4.2.5-delivery-contracts/docs/guide/implementations/rabbitmq-delivery-safety.md)
- [Branch guide: testing and verification](https://github.com/FoundatioFx/Foundatio/blob/docs/rabbitmq-4.2.5-delivery-contracts/docs/guide/implementations/rabbitmq-verification.md)

The branch guides describe the companion implementation, including changed exhaustion behavior and opt-in strict delivery contracts. They are not a claim that those APIs are already released. Review the two PRs together and coordinate documentation publication with the provider release. Keep these branch links available until the corresponding published guides exist.

**Breaking behavior:** exhausted `Automatic` deliveries now remain unacknowledged when no terminal destination is configured, instead of being silently discarded. This can block consumption and grow the broker backlog. Configure quarantine and capacity policies, or explicitly choose `DiscardOnDeliveryLimit` for discardable messages. `FireAndForget` remains the default; `RequireSuccessfulDispatch` requires Automatic, a configured `DeadLetterExchange`, and no discard.

Classic and quorum queues both support provider-confirmed retries/terminal handling. Replication and broker-managed at-least-once dead-lettering are quorum capabilities; changing a builder option cannot convert an existing classic queue.

### Delayed message delivery

`PublishAsync` supports `MessageOptions.DeliveryDelay`. On RabbitMQ before 4.3, the archived [`rabbitmq_delayed_message_exchange` plugin](https://github.com/rabbitmq/rabbitmq-delayed-message-exchange/) provides broker-side scheduling when installed. Without that plugin, the default is an in-process scheduler: pending messages are lost if the publisher stops. RabbitMQ 4.3 removed the Mnesia store required by the plugin, so the provider skips its probe and uses the in-process fallback. If the server version cannot be read, it attempts the probe before falling back.

Set `RequireBrokerDelayedDelivery` with durable messages and publisher confirms to reject a delayed publication when broker-side scheduling is unavailable. This option does not make the plugin replicated. Native quorum delayed retries on RabbitMQ 4.3+ apply to returned deliveries; they do not schedule the initial publication. For durable initial scheduling without the plugin, use an application outbox or a separately verified scheduler.

### RabbitMQ version and queue features

The library supports classic and quorum queues on the pinned 4.2.5 test baseline and retains guards for RabbitMQ 4.3+ features:

| Feature | RabbitMQ 4.2.x | RabbitMQ 4.3+ |
|---------|----------------|---------------|
| Classic priorities | `UseMessagePriority()` sets `x-max-priority` | Same |
| Quorum priorities | Normal/high tiers | 32 strict levels, built in |
| Quorum delayed retries | Unavailable | `UseDelayedRetries()` with linear backoff on returned deliveries |
| Quorum consumer timeout | Unavailable | `ConsumerTimeout()` |
| Single active consumer | Supported | Supported |

`UseMessagePriority()` configures classic queues only and fails when combined with `UseQuorumQueues()`. Quorum priorities are built in; publishers can still set the message `Priority` in either mode.

These features use the AMQP 0.9.1 client. AMQP 1.0 delivery annotations, rejected-by details, and consumer activity notifications are outside this provider's protocol. See the [RabbitMQ priority guide](https://www.rabbitmq.com/docs/priority) for the differences between classic and quorum queues.

### OpenTelemetry

Foundatio copies the current activity ID to the AMQP `CorrelationId` property unless the caller supplies one, carries `TraceState` in a header, and uses the received correlation ID as the parent of its handler activity. To collect handler spans, add the `Foundatio` activity source; RabbitMQ.Client 7.x also emits optional transport spans:

```csharp
services.AddOpenTelemetry().WithTracing(tracing =>
{
    tracing.AddSource("Foundatio");          // message handler spans
    tracing.AddSource("RabbitMQ.Client.*");  // AMQP transport spans (optional)
    tracing.AddOtlpExporter();
});
```

The transport instrumentation package is not required for Foundatio's application-level trace propagation.

## Development

All repository-managed broker configurations stay on **RabbitMQ 4.2.5**. The delayed-exchange plugin artifact is independently versioned `4.2.0`; no 4.3 broker upgrade is included. This compatibility pin is not a broker security/support-lifecycle certification.

From this repository root, with Docker and the required .NET SDK installed:

```bash
docker build -t foundatiorabbitmq-rabbitmq-delayed:latest build
dotnet build Foundatio.RabbitMQ.slnx --configuration Release
FOUNDATIO_RABBITMQ_REQUIRE_INFRASTRUCTURE=true \
  dotnet test --solution Foundatio.RabbitMQ.slnx --configuration Release --no-build
```

The existing shared **Build** runs the complete suite, including TLS, with the shared Aspire collection. Keep provider contract tests on the existing Foundatio bases and focused tests on `TestWithLoggingBase`. No separate endpoint, integration, TLS, or package-verification workflow is needed. See the linked verification guide for fixture ownership, test-only fault injection, and evidence boundaries.

### Aspire sample

After the image and solution build above:

```bash
dotnet run --project tests/Foundatio.RabbitMQ.AppHost --configuration Release --no-build --launch-profile http
```

The loopback HTTP profile starts a publisher and separate classic/quorum subscribers. Every fifth order deliberately fails and reaches its queue type's quarantine; successful orders drain normally. Sample source and quarantine queues have 16 MiB ready-message limits with reject-publish overflow. These limits do not cap total broker storage or unacknowledged work. Failed sample publications are logged, not replayed by an outbox.

Dashboard commands target this run's exact broker containers. Alarm commands capture the effective byte threshold and restore it when cleared; each Docker invocation has a 30-second deadline. Restore alarms before ending a scenario. For topology names, backlog/recovery scenarios, and focused test commands, see the [verification guide](https://github.com/FoundatioFx/Foundatio/blob/docs/rabbitmq-4.2.5-delivery-contracts/docs/guide/implementations/rabbitmq-verification.md).

The full TLS suite temporarily adds its test CA to the current-user root store and removes it during cleanup. Linux CI exercises this path. macOS may deny write access to that store; a required-infrastructure startup failure is not a passing or skipped TLS verification.

### Core Features

- [Getting Started](https://foundatio.dev/guide/getting-started) - Installation and setup
- [Caching](https://foundatio.dev/guide/caching) - In-memory, Redis, and hybrid caching with invalidation
- [Queues](https://foundatio.dev/guide/queues) - FIFO message delivery with lock renewal and retry policies
- [Locks](https://foundatio.dev/guide/locks) - Distributed locking with null handling patterns
- [Messaging](https://foundatio.dev/guide/messaging) - Pub/sub with size limits and notification patterns
- [File Storage](https://foundatio.dev/guide/storage) - Unified file API across providers
- [Jobs](https://foundatio.dev/guide/jobs) - Background job processing and hosted service integration

### Advanced Topics

- [Resilience](https://foundatio.dev/guide/resilience) - Retry policies, circuit breakers, and timeouts
- [Serialization](https://foundatio.dev/guide/serialization) - Serializer configuration and performance
- [Dependency Injection](https://foundatio.dev/guide/dependency-injection) - DI setup and patterns
- [Configuration](https://foundatio.dev/guide/configuration) - Options and settings

## 📦 CI Packages (Feedz)

```bash
dotnet nuget add source https://f.feedz.io/foundatio/foundatio/nuget -n foundatio-feedz
dotnet add package Foundatio.RabbitMQ --prerelease
```

Or add to your `NuGet.config`:

```xml
<configuration>
  <packageSources>
    <add key="foundatio-feedz" value="https://f.feedz.io/foundatio/foundatio/nuget" />
  </packageSources>
  <packageSourceMapping>
    <packageSource key="foundatio-feedz">
      <package pattern="Foundatio.*" />
    </packageSource>
  </packageSourceMapping>
</configuration>
```

## 🤝 Contributing

Contributions are welcome! See [AGENTS.md](AGENTS.md) for repository conventions and [issue #99](https://github.com/FoundatioFx/Foundatio.RabbitMQ/issues/99) for delivery/recovery tracking. Detailed provider documentation changes belong on a linked branch in FoundatioFx/Foundatio rather than a duplicate guide tree here.

## 📄 License

Apache 2.0 License. See [LICENSE.txt](LICENSE.txt).

## Thanks to all the people who have contributed

[![contributors](https://contributors-img.web.app/image?repo=foundatiofx/Foundatio.RabbitMQ)](https://github.com/foundatiofx/Foundatio.RabbitMQ/graphs/contributors)
