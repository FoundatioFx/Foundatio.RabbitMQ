using System;
using System.IO;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Foundatio.Messaging;

// Test-only child process. A parent integration test kills this process after the
// broker confirms a delayed publication and before the delivery deadline.
using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
string connectionString = Required("FOUNDATIO_PROBE_URI");
var uri = new Uri(connectionString);
if (!uri.IsLoopback || uri.Scheme is not ("amqp" or "amqps"))
    throw new InvalidOperationException("The process probe requires a test-owned loopback broker.");
string topic = Required("FOUNDATIO_PROBE_TOPIC");
string id = Required("FOUNDATIO_PROBE_ID");
string checkpoint = Required("FOUNDATIO_PROBE_CHECKPOINT");
if (!Path.IsPathFullyQualified(checkpoint))
    throw new InvalidOperationException("The test checkpoint must have an absolute path.");

await using var bus = new RabbitMQMessageBus(new RabbitMQMessageBusOptions
{
    ConnectionString = connectionString,
    Topic = topic,
    IsDurable = true,
    PublisherConfirmsEnabled = true,
    RequireBrokerDelayedDelivery = true
});
var startedAt = DateTimeOffset.UtcNow;
await bus.PublishAsync(new ProcessMessage { Id = id }, new MessageOptions
{
    UniqueId = id,
    DeliveryDelay = TimeSpan.FromSeconds(30)
}, timeout.Token);
string temporary = checkpoint + ".tmp";
await File.WriteAllTextAsync(temporary, JsonSerializer.Serialize(new { Id = id, StartedAt = startedAt }), timeout.Token);
File.Move(temporary, checkpoint);
Console.WriteLine($"Broker accepted delayed event {id}; parent may terminate this process.");
await Task.Delay(Timeout.InfiniteTimeSpan, timeout.Token);

static string Required(string name) => Environment.GetEnvironmentVariable(name)
    ?? throw new InvalidOperationException($"Missing process-probe setting {name}.");

public sealed class ProcessMessage
{
    public string Id { get; set; } = String.Empty;
}
