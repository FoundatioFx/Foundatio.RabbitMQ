using Projects;

var builder = DistributedApplication.CreateBuilder(args);

builder.AddRabbitMQ("messaging")
    .WithManagementPlugin()
    .WithImageTag("4.2.5-management");

builder.AddContainer("messaging-delayed", "foundatiorabbitmq-rabbitmq-delayed", "latest")
    .WithEndpoint(targetPort: 5672, name: "amqp", scheme: "tcp")
    .WithEndpoint(targetPort: 15672, name: "management", scheme: "http");

var containerMemoryLimits = new[] { "384m", "448m", "512m" };
var chaosHostnames = new[] { "chaos1", "chaos2", "chaos3" };
string runId = Guid.NewGuid().ToString("N");
var commands = new Dictionary<string, BrokerCommands>();
var chaosNodes = new List<IResourceBuilder<ContainerResource>>(3);

for (int nodeIndex = 0; nodeIndex < 3; nodeIndex++)
{
    string hostname = chaosHostnames[nodeIndex];
    string containerName = $"foundatio-chaos-{nodeIndex + 1}-{runId}";
    var chaosNode = builder.AddContainer($"chaos-{nodeIndex + 1}", "rabbitmq", "4.2.5-management")
        .WithContainerName(containerName)
        .WithContainerNetworkAlias(hostname)
        .WithContainerRuntimeArgs($"--memory={containerMemoryLimits[nodeIndex]}", "--hostname", hostname)
        .WithEnvironment("RABBITMQ_DEFAULT_USER", "guest")
        .WithEnvironment("RABBITMQ_DEFAULT_PASS", "guest")
        .WithEnvironment("RABBITMQ_NODENAME", $"rabbit@{hostname}")
        .WithEnvironment("RABBITMQ_ERLANG_COOKIE", "aspire-chaos-cluster-cookie")
        .WithBindMount($"config/chaos-{nodeIndex + 1}.conf", "/etc/rabbitmq/conf.d/99-limits.conf", isReadOnly: true)
        .WithEndpoint(targetPort: 5672, name: "amqp", scheme: "tcp")
        .WithEndpoint(targetPort: 15672, name: "management", scheme: "http")
        .WithEndpoint(targetPort: 15692, name: "prometheus", scheme: "http")
        .WithHttpHealthCheck("/metrics", endpointName: "prometheus");

    if (nodeIndex > 0)
        chaosNode.WaitFor(chaosNodes[0]);

    chaosNodes.Add(chaosNode);
    commands.Add(chaosNode.Resource.Name, new BrokerCommands(containerName));
}

var chaos1Amqp = chaosNodes[0].GetEndpoint("amqp");
var chaos2Amqp = chaosNodes[1].GetEndpoint("amqp");
var chaos3Amqp = chaosNodes[2].GetEndpoint("amqp");

var publisher = builder.AddProject<Foundatio_RabbitMQ_Publish>("publisher")
    .WaitFor(chaosNodes[0])
    .WaitFor(chaosNodes[1])
    .WaitFor(chaosNodes[2])
    .WithArgs("--interval", "2000", "--publisher-confirms", "--durable", "--require-routing")
    .WithEnvironment(context =>
    {
        context.EnvironmentVariables["ConnectionStrings__messaging"] =
            ReferenceExpression.Create($"amqp://guest:guest@{chaos1Amqp.Property(EndpointProperty.Host)}:{chaos1Amqp.Property(EndpointProperty.Port)}");
        context.EnvironmentVariables["RABBITMQ_HOSTS"] =
            ReferenceExpression.Create($"{chaos1Amqp.Property(EndpointProperty.Host)}:{chaos1Amqp.Property(EndpointProperty.Port)},{chaos2Amqp.Property(EndpointProperty.Host)}:{chaos2Amqp.Property(EndpointProperty.Port)},{chaos3Amqp.Property(EndpointProperty.Host)}:{chaos3Amqp.Property(EndpointProperty.Port)}");
    });

foreach (string queueType in new[] { "classic", "quorum" })
{
    var subscriber = builder.AddProject<Foundatio_RabbitMQ_Subscribe>($"subscriber-{queueType}")
        .WaitFor(chaosNodes[0])
        .WaitFor(chaosNodes[1])
        .WaitFor(chaosNodes[2])
        .WithArgs("--durable", "--queue-type", queueType, "--group-id", $"sample-{queueType}",
            "--acknowledgment-strategy", "automatic", "--require-successful-dispatch",
            "--dead-letter-exchange", $"sample-{queueType}-quarantine", "--provision-quarantine", "--fail-every", "5")
        .WithEnvironment(context =>
        {
            context.EnvironmentVariables["ConnectionStrings__messaging"] =
                ReferenceExpression.Create($"amqp://guest:guest@{chaos1Amqp.Property(EndpointProperty.Host)}:{chaos1Amqp.Property(EndpointProperty.Port)}");
            context.EnvironmentVariables["RABBITMQ_HOSTS"] =
                ReferenceExpression.Create($"{chaos1Amqp.Property(EndpointProperty.Host)}:{chaos1Amqp.Property(EndpointProperty.Port)},{chaos2Amqp.Property(EndpointProperty.Host)}:{chaos2Amqp.Property(EndpointProperty.Port)},{chaos3Amqp.Property(EndpointProperty.Host)}:{chaos3Amqp.Property(EndpointProperty.Port)}");
        });

    publisher.WaitFor(subscriber);
}

foreach (var node in chaosNodes)
{
    var broker = commands[node.Resource.Name];
    AddChaosCommand(node, "trigger-disk-alarm", "Trigger Disk Alarm", token => broker.SetAlarmAsync(false, true, token));
    AddChaosCommand(node, "clear-disk-alarm", "Restore Disk Limit", token => broker.SetAlarmAsync(false, false, token));
    AddChaosCommand(node, "trigger-memory-alarm", "Trigger Memory Alarm", token => broker.SetAlarmAsync(true, true, token));
    AddChaosCommand(node, "clear-memory-alarm", "Restore Memory Limit", token => broker.SetAlarmAsync(true, false, token));
    AddChaosCommand(node, "close-all-connections", "Close Connections", async token =>
    {
        _ = await broker.ExecuteAsync(["rabbitmqctl", "close_all_connections", "sample-recovery"], token);
    });
}

AddChaosCommand(chaosNodes[0], "trigger-all-disk-alarms", "Trigger All Disk Alarms",
    token => Task.WhenAll(commands.Values.Select(broker => broker.SetAlarmAsync(false, true, token))));
AddChaosCommand(chaosNodes[0], "clear-all-disk-alarms", "Restore All Disk Limits",
    token => Task.WhenAll(commands.Values.Select(broker => broker.SetAlarmAsync(false, false, token))));

await builder.Build().RunAsync();

static void AddChaosCommand(IResourceBuilder<ContainerResource> node, string name, string display, Func<CancellationToken, Task> execute)
{
    node.WithCommand(name, display, async context =>
    {
        try
        {
            await execute(context.CancellationToken);
            return CommandResults.Success();
        }
        catch (Exception exception)
        {
            return CommandResults.Failure(exception.Message);
        }
    }, new() { IconName = "Warning", ConfirmationMessage = $"{display} on this sample's broker resources?" });
}
