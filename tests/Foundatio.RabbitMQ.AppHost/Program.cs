using Projects;

var builder = DistributedApplication.CreateBuilder(args);

var messaging = builder.AddRabbitMQ("messaging")
    .WithManagementPlugin();

var messagingDelayed = builder.AddContainer("messaging-delayed", "foundatiorabbitmq-rabbitmq-delayed", "latest")
    .WithEndpoint(targetPort: 5672, name: "amqp", scheme: "tcp")
    .WithEndpoint(targetPort: 15672, name: "management", scheme: "http");

var chaosNode1 = builder.AddContainer("chaos-1", "rabbitmq", "4.2.2-management")
    .WithContainerRuntimeArgs("--memory=384m")
    .WithEnvironment("RABBITMQ_DEFAULT_USER", "guest")
    .WithEnvironment("RABBITMQ_DEFAULT_PASS", "guest")
    .WithBindMount("../../chaos-testing/config/chaos-1.conf", "/etc/rabbitmq/conf.d/99-limits.conf", isReadOnly: true)
    .WithEndpoint(targetPort: 5672, name: "amqp", scheme: "tcp")
    .WithEndpoint(targetPort: 15672, name: "management", scheme: "http");

var chaosNode2 = builder.AddContainer("chaos-2", "rabbitmq", "4.2.2-management")
    .WithContainerRuntimeArgs("--memory=448m")
    .WithEnvironment("RABBITMQ_DEFAULT_USER", "guest")
    .WithEnvironment("RABBITMQ_DEFAULT_PASS", "guest")
    .WithBindMount("../../chaos-testing/config/chaos-2.conf", "/etc/rabbitmq/conf.d/99-limits.conf", isReadOnly: true)
    .WithEndpoint(targetPort: 5672, name: "amqp", scheme: "tcp")
    .WithEndpoint(targetPort: 15672, name: "management", scheme: "http")
    .WaitFor(chaosNode1);

var chaosNode3 = builder.AddContainer("chaos-3", "rabbitmq", "4.2.2-management")
    .WithContainerRuntimeArgs("--memory=512m")
    .WithEnvironment("RABBITMQ_DEFAULT_USER", "guest")
    .WithEnvironment("RABBITMQ_DEFAULT_PASS", "guest")
    .WithBindMount("../../chaos-testing/config/chaos-3.conf", "/etc/rabbitmq/conf.d/99-limits.conf", isReadOnly: true)
    .WithEndpoint(targetPort: 5672, name: "amqp", scheme: "tcp")
    .WithEndpoint(targetPort: 15672, name: "management", scheme: "http")
    .WaitFor(chaosNode1);

builder.AddProject<Foundatio_RabbitMQ_Publish>("publisher")
    .WithReference(messaging)
    .WaitFor(messaging);

builder.AddProject<Foundatio_RabbitMQ_Subscribe>("subscriber")
    .WithReference(messaging)
    .WaitFor(messaging);

builder.Build().Run();
