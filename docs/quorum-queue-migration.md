# Migrating from Classic Queues to Quorum Queues

This guide covers migrating Foundatio.RabbitMQ consumers from classic queues to quorum queues.

## Why Migrate?

- **High availability**: Quorum queues replicate across cluster nodes and remain available with majority (2 of 3)
- **Rolling upgrade survival**: Classic queues go offline when their host node restarts; quorum queues do not
- **Native poison message handling**: Built-in delivery limit tracking without republish hacks
- **Native delayed retries** (4.3+): Linear backoff without the delayed message exchange plugin

## Prerequisites

- RabbitMQ 4.0+ (quorum queues are fully supported)
- Cluster with 3+ nodes (quorum requires majority)
- Queues must be durable, non-exclusive, and non-auto-delete

## Key Constraint

**You cannot convert an existing classic queue to quorum in-place.** RabbitMQ enforces strict queue argument validation. Attempting to redeclare a classic queue with `x-queue-type=quorum` results in a `PRECONDITION_FAILED` channel error (406).

## Migration Approaches

### Option 1: Delete and Recreate (Simplest)

Best for queues that can tolerate brief downtime and message loss is acceptable.

1. Stop all consumers for the queue
2. Delete the classic queue via management UI or CLI
3. Update your code to use `UseQuorumQueues()`
4. Deploy consumers — they will declare the new quorum queue

```csharp
var messageBus = new RabbitMQMessageBus(o => o
    .ConnectionString("amqp://...")
    .SubscriptionQueueName("my-queue")
    .UseQuorumQueues()  // adds x-queue-type=quorum
    .PrefetchCount(10));
```

### Option 2: New Queue Name (Zero Downtime)

Best for always-on queues where you can coordinate producer/consumer deployment.

1. Deploy new consumers with a different queue name (e.g., `my-queue-v2`)
2. Configure `UseQuorumQueues()` on the new consumers
3. Switch producers to publish to the new topic/exchange
4. Drain the old classic queue
5. Delete the old queue when empty

### Option 3: Server-Side Default Queue Type

Best when you control the RabbitMQ cluster and don't want to change application code.

1. Create a new vhost with `default_queue_type = quorum`
2. Move your connection string to the new vhost
3. Queues will be created as quorum automatically (except exclusive/auto-delete)

```ini
# rabbitmq.conf
default_queue_type = quorum
```

### Option 4: Relaxed Property Equivalence (Transitional)

For environments where applications explicitly set `x-queue-type=classic` and cannot be quickly updated. Available on RabbitMQ 4.0+.

```ini
# rabbitmq.conf - suppresses x-queue-type mismatch errors during redeclaration
quorum_queue.property_equivalence.relaxed_checks_on_redeclaration = true
```

**Warning**: This only suppresses the error. It does NOT convert existing queues. You must still delete and recreate them.

### Option 5: Blue-Green Deployment (Enterprise)

Best for large-scale migrations with zero message loss requirement.

1. Stand up a new RabbitMQ cluster ("green") with `default_queue_type = quorum`
2. Enable queue federation from old cluster ("blue") to green
3. Move consumers to green first (federation pulls from blue)
4. Move producers to green
5. Wait for blue queues to drain
6. Decommission blue

## Code Changes Required

### Before (Classic Queue)

```csharp
var messageBus = new RabbitMQMessageBus(o => o
    .ConnectionString("amqp://...")
    .SubscriptionQueueName("my-job-queue")
    .IsSubscriptionQueueExclusive(false)
    .SubscriptionQueueAutoDelete(false)
    .AcknowledgementStrategy(AcknowledgementStrategy.Automatic));
```

### After (Quorum Queue)

```csharp
var messageBus = new RabbitMQMessageBus(o => o
    .ConnectionString("amqp://...")
    .SubscriptionQueueName("my-job-queue")
    .UseQuorumQueues()  // sets durable, non-exclusive, non-auto-delete + x-queue-type=quorum
    .AcknowledgementStrategy(AcknowledgementStrategy.Automatic)
    .PrefetchCount(10)  // recommended: quorum queues benefit from prefetch > 0
    .DeliveryLimit(5)); // optional: configure poison message handling
```

### With Delayed Retries (4.3+)

```csharp
var messageBus = new RabbitMQMessageBus(o => o
    .ConnectionString("amqp://...")
    .SubscriptionQueueName("my-job-queue")
    .UseQuorumQueues()
    .UseDelayedRetries(minDelayMs: 1000, maxDelayMs: 60000)
    .AcknowledgementStrategy(AcknowledgementStrategy.Automatic)
    .PrefetchCount(10));
```

## Incompatible Features

The following classic queue features are **not supported** by quorum queues:

| Feature | Alternative |
|---------|-------------|
| `exclusive=true` | Not supported. Use `x-single-active-consumer` for ordering |
| `autoDelete=true` | Not supported. Use queue TTL (`x-expires`) |
| Global QoS | Use per-consumer QoS (default behavior) |
| `x-queue-mode=lazy` | Not applicable (quorum queues manage memory natively) |
| `x-max-priority` (classic) | Supported on 4.3+ with 32 strict priority levels |
| `x-overflow=reject-publish-dlx` | Use `reject-publish` instead |

## Verifying Migration

After migration, verify:

1. Queue shows as "quorum" type in RabbitMQ Management UI
2. Queue has replicas on multiple nodes (check "Members" in UI)
3. Consumers receive messages correctly
4. Publisher confirms work (if enabled)
5. Delivery limit triggers correctly on poison messages

## Troubleshooting

### PRECONDITION_FAILED on startup

The classic queue still exists. Delete it first, or use a new queue name.

### Consumer timeout channel errors

Quorum queues on 4.3+ evaluate consumer timeouts (default 30 min). If your handlers are slow, increase the timeout via broker config:

```ini
# rabbitmq.conf
consumer_timeout = 3600000  # 1 hour in ms
```

### Reduced throughput vs classic

Expected. Quorum queues replicate data via Raft consensus. Mitigate by:
- Increasing `PrefetchCount` (10-50 for most workloads)
- Using publisher confirms in async/batch mode
- Partitioning hot queues across multiple queue names
