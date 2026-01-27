# Kill a cluster node
# Usage: ./KillNode.ps1 [node] [-Restart]
# If no node specified, kills the first running node
param(
    [int]$Node = 0,
    [switch]$Restart
)

# Find first running node if not specified
if ($Node -eq 0) {
    foreach ($i in 1..3) {
        $running = docker ps --filter "name=chaos-rabbitmq-$i" --format "{{.Names}}" 2>$null
        if ($running) {
            $Node = $i
            break
        }
    }
    if ($Node -eq 0) {
        Write-Host "  No running nodes found." -ForegroundColor Yellow
        exit 0
    }
}

$Container = "chaos-rabbitmq-$Node"

# Check if already dead
$running = docker ps --filter "name=$Container" --format "{{.Names}}" 2>$null
if (-not $running) {
    Write-Host "  Node $Node is already down." -ForegroundColor Yellow
    exit 0
}

Write-Host "  Killing node $Node..." -ForegroundColor Red
docker kill $Container 2>$null | Out-Null

if ($Restart) {
    Write-Host "  Waiting 5s..." -ForegroundColor Gray
    Start-Sleep -Seconds 5
    Write-Host "  Restarting node $Node..." -ForegroundColor Green
    docker start $Container | Out-Null
}

# Show current state
Write-Host ""
& "$PSScriptRoot/Status.ps1"
