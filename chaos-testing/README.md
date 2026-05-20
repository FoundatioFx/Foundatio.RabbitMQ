# RabbitMQ Chaos Testing

A 3-node RabbitMQ cluster with constrained resources for testing failure scenarios.

## Architecture

The chaos testing infrastructure has two modes of operation:

1. **Automated (Aspire)** - Integration tests run via `dotnet test` using .NET Aspire to orchestrate containers. No manual setup required.
2. **Manual (Scripts)** - PowerShell scripts for interactive chaos testing during development.
3. **AI-Agent (MCP)** - MCP tool descriptors allow AI coding agents to invoke chaos operations against a running Aspire AppHost.

### How It Works

```
┌─────────────────────────────────────────────────────────────┐
│  Aspire AppHost (tests/Foundatio.RabbitMQ.AppHost)          │
│                                                             │
│  ┌───────────┐  ┌───────────┐  ┌───────────┐              │
│  │  chaos-1  │  │  chaos-2  │  │  chaos-3  │              │
│  │  384MB    │  │  448MB    │  │  512MB    │              │
│  │  50MB disk│  │  75MB disk│  │  100MB disk│             │
│  └───────────┘  └───────────┘  └───────────┘              │
│         └──────────────┼──────────────┘                    │
│                   RabbitMQ Cluster                          │
│              (Erlang cookie: chaos-testing-cookie)          │
└─────────────────────────────────────────────────────────────┘
         ▲                    ▲                    ▲
         │                    │                    │
    ChaosTestHelper      MCP Tools           Manual Scripts
    (dotnet test)        (AI agents)         (PowerShell)
```

## Quick Start (Automated Tests)

```bash
# Run all chaos tests (requires Docker)
dotnet test chaos-testing/Foundatio.RabbitMQ.ChaosTests

# Run a specific test
dotnet test chaos-testing/Foundatio.RabbitMQ.ChaosTests --filter "FillDisk_TriggersDiskAlarm"

# Run integration tests (uses Aspire for RabbitMQ)
dotnet test tests/Foundatio.RabbitMQ.Tests
```

## Quick Start (Aspire Dashboard)

```bash
# Start the full AppHost with dashboard
dotnet run --project tests/Foundatio.RabbitMQ.AppHost

# The Aspire dashboard is available at https://localhost:15888
# Use MCP tools or manual docker commands while AppHost is running
```

## MCP Tools (AI-Agent Driven Chaos)

The `mcp-tools/` directory contains MCP tool descriptors and a runner script for AI-agent-driven chaos testing.

### Prerequisites

- Docker running
- Aspire AppHost started: `dotnet run --project tests/Foundatio.RabbitMQ.AppHost`

### Available Tools

| Tool | Description |
|------|-------------|
| `chaos_fill_disk` | Fill tmpfs to trigger disk alarm (blocks publishes) |
| `chaos_clear_disk` | Remove fill file to allow alarm to clear |
| `chaos_stop_node` | Kill a node container (hard failure) |
| `chaos_start_node` | Restart a stopped node |
| `chaos_check_alarm` | Query disk alarm state |
| `chaos_cluster_status` | Get cluster membership and health |

### Usage

```bash
# Fill disk on node 1 (triggers alarm in ~5s)
./chaos-testing/mcp-tools/run-chaos-tool.sh fill_disk chaos-1

# Check if alarm is active
./chaos-testing/mcp-tools/run-chaos-tool.sh check_alarm chaos-1

# Clear disk (alarm clears in ~10-15s)
./chaos-testing/mcp-tools/run-chaos-tool.sh clear_disk chaos-1

# Kill a node
./chaos-testing/mcp-tools/run-chaos-tool.sh stop_node chaos-2

# Restart a node
./chaos-testing/mcp-tools/run-chaos-tool.sh start_node chaos-2

# Get cluster status
./chaos-testing/mcp-tools/run-chaos-tool.sh cluster_status chaos-1
```

---

## Sample App Command-Line Options

### Publisher Options

| Option | Description | Default |
|--------|-------------|---------|
| `--connection-string` | RabbitMQ connection string (credentials and vhost) | `amqp://localhost:5672` |
| `--hosts` | Comma-separated list of hosts for failover | _(none)_ |
| `--topic` | Message topic/exchange name | `sample-topic` |
| `--durable` | Use durable queues that survive broker restarts | `false` |
| `--delayed` | Use delayed exchange (connects to port 5673) | `false` |
| `--acknowledgment-strategy` | `fireandforget` or `automatic` | `fireandforget` |
| `--publisher-confirms` | Wait for broker confirmation before returning (guarantees delivery) | `false` |
| `--message-size` | Target message size in bytes (pads Notes field) | `0` |
| `--prefetch-count` | Consumer prefetch count | `10` |
| `--delivery-limit` | Maximum delivery attempts before discarding | `2` |
| `--delay-seconds` | Delay in seconds before message delivery | `0` |
| `--interval` | Auto-send interval in ms (0 = manual mode) | `0` |
| `--log-level` | Log level: `Trace`, `Debug`, `Information`, `Warning`, `Error`, `Critical`, `None` | `Information` |

### Subscriber Options

| Option | Description | Default |
|--------|-------------|---------|
| `--connection-string` | RabbitMQ connection string (credentials and vhost) | `amqp://localhost:5672` |
| `--hosts` | Comma-separated list of hosts for failover | _(none)_ |
| `--topic` | Message topic/exchange name | `sample-topic` |
| `--durable` | Use durable queues that survive broker restarts | `false` |
| `--delayed` | Use delayed exchange (connects to port 5673) | `false` |
| `--acknowledgment-strategy` | `fireandforget` or `automatic` | `fireandforget` |
| `--prefetch-count` | Consumer prefetch count | `10` |
| `--delivery-limit` | Maximum delivery attempts before discarding | `2` |
| `--subscriber-count` | Number of concurrent subscribers | `1` |
| `--group-id` | Subscriber group identifier for queue naming | `sample-subscriber` |
| `--log-level` | Log level: `Trace`, `Debug`, `Information`, `Warning`, `Error`, `Critical`, `None` | `Information` |

---

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

### Failover connection (all 3 nodes)

```powershell
cd samples/Foundatio.RabbitMQ.Subscribe
dotnet run --hosts "localhost:5672,localhost:5673,localhost:5674"
```

```powershell
cd samples/Foundatio.RabbitMQ.Publish
dotnet run --hosts "localhost:5672,localhost:5673,localhost:5674" --interval 1000
```

The `--hosts` option provides a list of hosts to try in order. If the first host is unavailable, it tries the next one.

The `--interval` option auto-sends messages at the specified interval (in milliseconds). Useful for watching message flow.

---

### Connect to specific node

```powershell
cd samples/Foundatio.RabbitMQ.Subscribe
dotnet run --connection-string "amqp://guest:guest@localhost:5673"
```

```powershell
cd samples/Foundatio.RabbitMQ.Publish
dotnet run --connection-string "amqp://guest:guest@localhost:5673"
```

---

### Durable queues (survive restarts)

```powershell
cd samples/Foundatio.RabbitMQ.Subscribe
dotnet run --durable
```

```powershell
cd samples/Foundatio.RabbitMQ.Publish
dotnet run --durable
```

---

### Durable + failover

```powershell
cd samples/Foundatio.RabbitMQ.Subscribe
dotnet run --hosts "localhost:5672,localhost:5673,localhost:5674" --durable
```

```powershell
cd samples/Foundatio.RabbitMQ.Publish
dotnet run --hosts "localhost:5672,localhost:5673,localhost:5674" --durable
```

---

### Large messages (1MB - memory pressure)

```powershell
cd samples/Foundatio.RabbitMQ.Publish
dotnet run --message-size 1048576
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

## Chaos Commands

Run these while your apps are connected:

### Disk Alarm Testing (Connection Blocked)

When RabbitMQ's disk alarm triggers, the broker sends a `connection.blocked` notification. The Foundatio.RabbitMQ library now **throws a `MessageBusException`** when attempting to publish on a blocked connection, preventing silent message loss.

**Test this behavior:**

```powershell
# Terminal 1: Start subscriber
cd samples/Foundatio.RabbitMQ.Subscribe
dotnet run --hosts "localhost:5672,localhost:5673,localhost:5674"

# Terminal 2: Start publisher with auto-send
cd samples/Foundatio.RabbitMQ.Publish
dotnet run --hosts "localhost:5672,localhost:5673,localhost:5674" --interval 1000

# Terminal 3: Trigger disk alarm
cd chaos-testing
./scripts/FillDisk.ps1
```

**Expected behavior:**
- Publisher logs: `Publisher connection blocked: low disk space`
- Subsequent publish attempts throw `MessageBusException: Cannot publish: publisher connection is blocked by broker (resource alarm)`
- After clearing disk: `Publisher connection unblocked` and publishing resumes

```powershell
./scripts/ClearDisk.ps1
```

**Official Documentation:**
- [Blocked Connection Notifications](https://www.rabbitmq.com/docs/connection-blocked)
- [Memory and Disk Alarms](https://www.rabbitmq.com/docs/alarms)

---

### Other Chaos Commands

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

---

## Automated Test Architecture

The `Foundatio.RabbitMQ.ChaosTests` project contains xUnit tests that automatically:

1. Start a 3-node RabbitMQ cluster via Aspire
2. Inject faults (disk pressure, node kills) using Docker commands
3. Verify the Foundatio.RabbitMQ library handles faults correctly
4. Clean up all resources when tests complete

### Key Components

| Component | Path | Purpose |
|-----------|------|---------|
| AppHost | `tests/Foundatio.RabbitMQ.AppHost/` | Aspire orchestrator defining all containers |
| ChaosTestHelper | `chaos-testing/.../ChaosTestHelper.cs` | Docker exec wrapper for fault injection |
| ChaosFixture | `chaos-testing/.../ChaosIntegrationTests.cs` | xUnit fixture managing AppHost lifecycle |
| Config files | `chaos-testing/config/chaos-*.conf` | Per-node RabbitMQ cluster config |

### Test Cases

| Test | Validates |
|------|-----------|
| `FillDisk_TriggersDiskAlarm` | Disk alarm activates/clears correctly |
| `PublishAsync_DuringDiskAlarm_BlocksUntilRecovery` | Library blocks publishes during alarm |
| `StopNode_MakesNodeUnreachable` | Library throws on dead connection |
| `ClusterStatus_ReturnsValidJson` | Cluster is properly formed |

### CI Integration

Chaos tests run on `main` branch pushes only (not PRs) due to their longer runtime (~2-3 minutes). See `.github/workflows/build.yml`.

