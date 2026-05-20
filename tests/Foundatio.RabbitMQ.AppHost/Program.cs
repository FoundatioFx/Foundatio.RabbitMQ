using Projects;

var builder = DistributedApplication.CreateBuilder(args);

// --- Standard Integration Test Resources ---

var messaging = builder.AddRabbitMQ("messaging")
    .WithManagementPlugin();

var messagingDelayed = builder.AddContainer("messaging-delayed", "foundatiorabbitmq-rabbitmq-delayed", "latest")
    .WithEndpoint(targetPort: 5672, name: "amqp", scheme: "tcp")
    .WithEndpoint(targetPort: 15672, name: "management", scheme: "http");

// --- Chaos Testing Resources (3-node constrained cluster) ---

var chaosNode1 = builder.AddContainer("chaos-1", "rabbitmq", "4.2.2-management")
    .WithContainerRuntimeArgs("--memory=384m")
    .WithContainerRuntimeArgs("--mount", "type=tmpfs,destination=/var/lib/rabbitmq,tmpfs-size=52428800,tmpfs-mode=1777")
    .WithEnvironment("RABBITMQ_DEFAULT_USER", "guest")
    .WithEnvironment("RABBITMQ_DEFAULT_PASS", "guest")
    .WithEnvironment("RABBITMQ_ERLANG_COOKIE", "chaos-testing-cookie")
    .WithEnvironment("RABBITMQ_NODENAME", "rabbit@chaos-1")
    .WithBindMount("../../chaos-testing/config/chaos-1.conf", "/etc/rabbitmq/conf.d/99-limits.conf", isReadOnly: true)
    .WithEndpoint(targetPort: 5672, name: "amqp", scheme: "tcp")
    .WithEndpoint(targetPort: 15672, name: "management", scheme: "http");

var chaosNode2 = builder.AddContainer("chaos-2", "rabbitmq", "4.2.2-management")
    .WithContainerRuntimeArgs("--memory=448m")
    .WithContainerRuntimeArgs("--mount", "type=tmpfs,destination=/var/lib/rabbitmq,tmpfs-size=78643200,tmpfs-mode=1777")
    .WithEnvironment("RABBITMQ_DEFAULT_USER", "guest")
    .WithEnvironment("RABBITMQ_DEFAULT_PASS", "guest")
    .WithEnvironment("RABBITMQ_ERLANG_COOKIE", "chaos-testing-cookie")
    .WithEnvironment("RABBITMQ_NODENAME", "rabbit@chaos-2")
    .WithBindMount("../../chaos-testing/config/chaos-2.conf", "/etc/rabbitmq/conf.d/99-limits.conf", isReadOnly: true)
    .WithEndpoint(targetPort: 5672, name: "amqp", scheme: "tcp")
    .WithEndpoint(targetPort: 15672, name: "management", scheme: "http")
    .WaitFor(chaosNode1);

var chaosNode3 = builder.AddContainer("chaos-3", "rabbitmq", "4.2.2-management")
    .WithContainerRuntimeArgs("--memory=512m")
    .WithContainerRuntimeArgs("--mount", "type=tmpfs,destination=/var/lib/rabbitmq,tmpfs-size=104857600,tmpfs-mode=1777")
    .WithEnvironment("RABBITMQ_DEFAULT_USER", "guest")
    .WithEnvironment("RABBITMQ_DEFAULT_PASS", "guest")
    .WithEnvironment("RABBITMQ_ERLANG_COOKIE", "chaos-testing-cookie")
    .WithEnvironment("RABBITMQ_NODENAME", "rabbit@chaos-3")
    .WithBindMount("../../chaos-testing/config/chaos-3.conf", "/etc/rabbitmq/conf.d/99-limits.conf", isReadOnly: true)
    .WithEndpoint(targetPort: 5672, name: "amqp", scheme: "tcp")
    .WithEndpoint(targetPort: 15672, name: "management", scheme: "http")
    .WaitFor(chaosNode1);

// --- Sample App Resources ---

builder.AddProject<Foundatio_RabbitMQ_Publish>("publisher")
    .WithReference(messaging)
    .WaitFor(messaging);

builder.AddProject<Foundatio_RabbitMQ_Subscribe>("subscriber")
    .WithReference(messaging)
    .WaitFor(messaging);

builder.Build().Run();
