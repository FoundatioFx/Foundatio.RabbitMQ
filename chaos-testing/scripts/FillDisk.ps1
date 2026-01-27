# Fill disk on a node to trigger disk alarm
# Creates a dummy file to consume disk space (does NOT affect RabbitMQ data)
# Usage: ./FillDisk.ps1 [node] [size-mb]
# If no node specified, fills the first running node
param(
    [int]$Node = 0,
    [int]$SizeMB = 0
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

# Default sizes based on node limits (fill most of available space)
if ($SizeMB -eq 0) {
    $SizeMB = switch ($Node) {
        1 { 45 }  # 50MB disk
        2 { 70 }  # 75MB disk
        3 { 95 }  # 100MB disk
        default { 45 }
    }
}

Write-Host "  Filling node $Node with ${SizeMB}MB dummy file..." -ForegroundColor Yellow
Write-Host "  (This triggers disk alarm but doesn't affect queued messages)" -ForegroundColor Gray
docker exec $Container dd if=/dev/zero of=/var/lib/rabbitmq/fill-test bs=1M count=$SizeMB 2>$null | Out-Null

Write-Host "  Done." -ForegroundColor Green
Write-Host ""

# Show status
& "$PSScriptRoot/Status.ps1"
