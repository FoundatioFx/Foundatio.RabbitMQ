# Clear disk fill test file from a node
# Removes the dummy file created by FillDisk.ps1 (does NOT affect RabbitMQ data)
# Usage: ./ClearDisk.ps1 [node]
# If no node specified, clears all nodes
param([int]$Node = 0)

if ($Node -eq 0) {
    Write-Host "  Clearing test files on all nodes..." -ForegroundColor Cyan
    foreach ($i in 1..3) {
        docker exec chaos-rabbitmq-$i rm -f /var/lib/rabbitmq/fill-test 2>$null | Out-Null
    }
} else {
    Write-Host "  Clearing test file on node $Node..." -ForegroundColor Cyan
    docker exec chaos-rabbitmq-$Node rm -f /var/lib/rabbitmq/fill-test 2>$null | Out-Null
}

Write-Host "  Done. Disk alarm should clear shortly." -ForegroundColor Green
Write-Host ""

# Show status
& "$PSScriptRoot/Status.ps1"
