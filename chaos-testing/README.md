# RabbitMQ Chaos Testing

A 3-node RabbitMQ cluster with constrained resources for testing failure scenarios.

## Cluster Nodes

| Node | AMQP Port | Management UI              | Disk Limit | Memory Limit |
|------|-----------|----------------------------|------------|--------------|
| 1    | 5672      | <http://localhost:15672>   | 50MB       | 384MB        |
| 2    | 5673      | <http://localhost:15673>   | 75MB       | 448MB        |
| 3    | 5674      | <http://localhost:15674>   | 100MB      | 512MB        |

Node 1 has the tightest limits and will fail first under load.

**Credentials:** `guest` / `guest`

## Quick Start

```powershell
./scripts/Start.ps1
./scripts/Status.ps1
./scripts/Stop.ps1
```

## Scripts

```powershell
./scripts/Start.ps1
./scripts/Stop.ps1
./scripts/Status.ps1
./scripts/FillDisk.ps1
./scripts/FillDisk.ps1 2
./scripts/FillDisk.ps1 1 40
./scripts/ClearDisk.ps1
./scripts/ClearDisk.ps1 2
./scripts/KillNode.ps1
./scripts/KillNode.ps1 2
./scripts/KillNode.ps1 1 -Restart
./scripts/Reset.ps1
```

---

## Test Scenarios

### Start cluster first

```powershell
cd chaos-testing
./scripts/Start.ps1
```

---

### Basic pub/sub

```powershell
cd samples/Foundatio.RabbitMQ.Subscribe
dotnet run
```

```powershell
cd samples/Foundatio.RabbitMQ.Publish
dotnet run
```

---

### Single node connection

```powershell
cd samples/Foundatio.RabbitMQ.Subscribe
dotnet run --connection-string "amqp://guest:guest@localhost:5672"
```

```powershell
cd samples/Foundatio.RabbitMQ.Publish
dotnet run --connection-string "amqp://guest:guest@localhost:5672"
```

---

### Failover connection (all 3 nodes)

```powershell
cd samples/Foundatio.RabbitMQ.Subscribe
dotnet run --connection-string "amqp://guest:guest@localhost:5672,localhost:5673,localhost:5674"
```

```powershell
cd samples/Foundatio.RabbitMQ.Publish
dotnet run --connection-string "amqp://guest:guest@localhost:5672,localhost:5673,localhost:5674"
```

---

### Durable queues (survive restarts)

```powershell
cd samples/Foundatio.RabbitMQ.Subscribe
dotnet run --connection-string "amqp://guest:guest@localhost:5672" --durable
```

```powershell
cd samples/Foundatio.RabbitMQ.Publish
dotnet run --connection-string "amqp://guest:guest@localhost:5672" --durable
```

---

### Large messages (1MB - memory pressure)

```powershell
cd samples/Foundatio.RabbitMQ.Publish
dotnet run --connection-string "amqp://guest:guest@localhost:5672" --message-size 1048576
```

---

### Delayed messages

```powershell
cd samples/Foundatio.RabbitMQ.Subscribe
dotnet run --delayed
```

```powershell
cd samples/Foundatio.RabbitMQ.Publish
dotnet run --delayed
```

---

### Durable + failover

```powershell
cd samples/Foundatio.RabbitMQ.Subscribe
dotnet run --connection-string "amqp://guest:guest@localhost:5672,localhost:5673,localhost:5674" --durable
```

```powershell
cd samples/Foundatio.RabbitMQ.Publish
dotnet run --connection-string "amqp://guest:guest@localhost:5672,localhost:5673,localhost:5674" --durable
```

---

## Chaos Commands

Run these while your apps are connected:

```powershell
./scripts/FillDisk.ps1
```

```powershell
./scripts/ClearDisk.ps1
```

```powershell
./scripts/KillNode.ps1
```

```powershell
./scripts/KillNode.ps1 1 -Restart
```

```powershell
./scripts/KillNode.ps1 1
./scripts/KillNode.ps1 2
```

```powershell
./scripts/FillDisk.ps1 1
./scripts/FillDisk.ps1 2
./scripts/FillDisk.ps1 3
```

```powershell
./scripts/Status.ps1
```
