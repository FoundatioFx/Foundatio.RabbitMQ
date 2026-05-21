# RabbitMQ Chaos Testing

Independent RabbitMQ nodes with constrained resources for testing failure scenarios.

## Architecture

The chaos testing infrastructure has three modes:

1. **Automated (Aspire)** - Integration tests via `dotnet test` using .NET Aspire to orchestrate containers.
2. **Manual (Scripts)** - PowerShell scripts for interactive chaos testing during development.
3. **AI-Agent (MCP)** - MCP tool descriptors for AI-agent-driven chaos against a running Aspire AppHost.

### How It Works

```
┌─────────────────────────────────────────────────────────────┐
│  Aspire AppHost (tests/Foundatio.RabbitMQ.AppHost)          │
│                                                             │
│  ┌───────────┐  ┌───────────┐  ┌───────────┐              │
│  │  chaos-1  │  │  chaos-2  │  │  chaos-3  │              │
│  │  384MB    │  │  448MB    │  │  512MB    │              │
│  │  10MB disk│  │  10MB disk│  │  10MB disk│             │
│  └───────────┘  └───────────┘  └───────────┘              │
│         │               │               │                   │
│    Independent     Independent     Independent              │
│    (no cluster)    (failover)      (spare)                  │
└─────────────────────────────────────────────────────────────┘
         ▲                    ▲                    ▲
         │                    │                    │
    ChaosTestHelper      MCP Tools           Manual Scripts
    (dotnet test)        (AI agents)         (PowerShell)
```

Nodes are independent (no clustering). Multi-endpoint failover is tested by connecting to chaos-2 when chaos-1 is killed.

## Quick Start (Automated Tests)

```bash
# Run all chaos tests (requires Docker)
dotnet test chaos-testing/Foundatio.RabbitMQ.ChaosTests/Foundatio.RabbitMQ.ChaosTests.csproj

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
| `chaos_fill_disk` | Set disk_free_limit to 999GB to trigger disk alarm (blocks publishes) |
| `chaos_clear_disk` | Reset disk_free_limit to 10MB to clear alarm |
| `chaos_stop_node` | Kill a node container (hard failure) |
| `chaos_start_node` | Restart a stopped node |
| `chaos_check_alarm` | Query disk alarm state |
| `chaos_cluster_status` | Get node health and status |

### Usage

```bash
# Trigger disk alarm on node 1
./chaos-testing/mcp-tools/run-chaos-tool.sh fill_disk chaos-1

# Check if alarm is active
./chaos-testing/mcp-tools/run-chaos-tool.sh check_alarm chaos-1

# Clear disk alarm
./chaos-testing/mcp-tools/run-chaos-tool.sh clear_disk chaos-1

# Kill a node
./chaos-testing/mcp-tools/run-chaos-tool.sh stop_node chaos-2

# Restart a node
./chaos-testing/mcp-tools/run-chaos-tool.sh start_node chaos-2

# Get node status
./chaos-testing/mcp-tools/run-chaos-tool.sh cluster_status chaos-1
```

---

## Nodes

| Node | Memory Limit | Disk Limit | Purpose |
|------|-------------|------------|---------|
| chaos-1 | 384MB | 10MB | Primary target for fault injection |
| chaos-2 | 448MB | 10MB | Failover target |
| chaos-3 | 512MB | 10MB | Additional failover |

**Credentials:** `guest` / `guest`

## Manual Scripts

```powershell
./scripts/Start.ps1
./scripts/Stop.ps1
./scripts/Status.ps1
./scripts/FillDisk.ps1 [node] [sizeMB]
./scripts/ClearDisk.ps1 [node]
./scripts/KillNode.ps1 [node] [-Restart]
./scripts/Reset.ps1
```

---

## Automated Test Scenarios

| Test | Validates |
|------|-----------|
| `SetDiskFreeLimit_WhenSetToUnreachableValue_TriggersAndClearsAlarm` | Disk alarm activates/clears correctly |
| `PublishAsync_DuringDiskAlarm_WithoutConfirms_MaySucceedSilently` | Fire-and-forget may not detect blocked state |
| `PublishAsync_WithPublisherConfirms_DuringDiskAlarm_FailsOrTimesOut` | Publisher confirms detect blocked connection |
| `PublishAsync_AfterNodeRestart_RecoversAutomatically` | Auto-recovery after node restart |
| `PublishAsync_NodeDown_RecoveryTimeoutExpires_ThrowsMessageBusException` | Timeout throws appropriately |
| `SubscribeAsync_ReceivesMessages_AfterNodeRestart` | Subscriber reconnects after restart |
| `CompetingConsumers_WhenMultipleSubscribersActive_DistributesMessagesEvenly` | Round-robin distribution |
| `SubscribeAsync_DuringRollingUpgrade_ResumesWithoutProcessRestart` | Full recovery audit trail |
| `PublishAsync_WhenPrimaryHostDown_ConnectsToAlternateEndpoint` | Multi-endpoint failover |
| `PublishAsync_ConcurrentDuringNodeKill_AllRecoverOrFailGracefully` | Concurrent publish during failure |
| `PublishAsync_DuringDiskAlarmAndNodeKill_RecoversAfterRestart` | Double-fault recovery |
| `PublishAsync_AfterRapidConnectionFlapping_SystemRemainsStable` | Stability after rapid reconnects |
| `PublishAsync_HighVolumeBurstAfterRecovery_AllMessagesDelivered` | No message loss post-recovery |
| `SubscribeAsync_NodeKillDuringSlowProcessing_SubscriptionSurvives` | Slow consumer survives restart |

### CI Integration

Chaos tests run on `main` branch pushes only (not PRs) due to their longer runtime. See `.github/workflows/build.yml`.
