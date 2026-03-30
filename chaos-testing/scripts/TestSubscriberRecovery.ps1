# TestSubscriberRecovery.ps1 - Automated chaos test for subscriber recovery
# Usage: ./scripts/TestSubscriberRecovery.ps1
# Runs the full disk-alarm + node-kill scenario and validates subscriber resumes

param(
    [int]$MessageIntervalMs = 2000,
    [int]$WaitBeforeChaosSeconds = 10,
    [int]$WaitAfterRecoverySeconds = 20,
    [string]$Hosts = "localhost:5672,localhost:5673,localhost:5674"
)

$ErrorActionPreference = "Stop"
$ScriptDir = $PSScriptRoot
$SamplesDir = Join-Path $ScriptDir "../../samples"
$SubscriberLog = Join-Path $ScriptDir "../subscriber-test.log"
$PublisherLog = Join-Path $ScriptDir "../publisher-test.log"
$SubscriberErrLog = Join-Path $ScriptDir "../subscriber-err.log"
$PublisherErrLog = Join-Path $ScriptDir "../publisher-err.log"

# Cleanup any previous log files
Remove-Item -Force $SubscriberLog -ErrorAction SilentlyContinue
Remove-Item -Force $PublisherLog -ErrorAction SilentlyContinue
Remove-Item -Force $SubscriberErrLog -ErrorAction SilentlyContinue
Remove-Item -Force $PublisherErrLog -ErrorAction SilentlyContinue

Write-Host "`n=== Subscriber Recovery Chaos Test ===" -ForegroundColor Cyan
Write-Host ""

# Step 1: Ensure cluster is running
Write-Host "  [1/7] Starting cluster..." -ForegroundColor Yellow
& "$ScriptDir/Start.ps1"

# Step 2: Start subscriber in background
Write-Host "  [2/7] Starting subscriber..." -ForegroundColor Yellow
$subscriberProcess = Start-Process -FilePath "dotnet" `
    -ArgumentList "run", "--", "--hosts", $Hosts, "--log-level", "Information" `
    -WorkingDirectory (Join-Path $SamplesDir "Foundatio.RabbitMQ.Subscribe") `
    -RedirectStandardOutput $SubscriberLog `
    -RedirectStandardError $SubscriberErrLog `
    -PassThru -NoNewWindow

Write-Host "    PID: $($subscriberProcess.Id)" -ForegroundColor Gray

# Step 3: Start publisher in background (auto-send)
Write-Host "  [3/7] Starting publisher (interval: ${MessageIntervalMs}ms)..." -ForegroundColor Yellow
$publisherProcess = Start-Process -FilePath "dotnet" `
    -ArgumentList "run", "--", "--hosts", $Hosts, "--interval", $MessageIntervalMs, "--log-level", "Information" `
    -WorkingDirectory (Join-Path $SamplesDir "Foundatio.RabbitMQ.Publish") `
    -RedirectStandardOutput $PublisherLog `
    -RedirectStandardError $PublisherErrLog `
    -PassThru -NoNewWindow

Write-Host "    PID: $($publisherProcess.Id)" -ForegroundColor Gray

# Step 4: Wait for baseline message flow
Write-Host "  [4/7] Waiting ${WaitBeforeChaosSeconds}s for baseline message flow..." -ForegroundColor Yellow
Start-Sleep -Seconds $WaitBeforeChaosSeconds

# Count baseline messages
$baselineCount = (Select-String -Path $SubscriberLog -Pattern "Order #" -ErrorAction SilentlyContinue).Count
Write-Host "    Baseline: $baselineCount messages received" -ForegroundColor Gray

if ($baselineCount -eq 0) {
    Write-Host "  FAIL: No messages received before chaos. Check logs." -ForegroundColor Red
    Write-Host "  Subscriber log: $SubscriberLog"
    Write-Host "  Publisher log: $PublisherLog"
    Stop-Process -Id $subscriberProcess.Id -Force -ErrorAction SilentlyContinue
    Stop-Process -Id $publisherProcess.Id -Force -ErrorAction SilentlyContinue
    exit 1
}

# Step 5: Run chaos scenario
Write-Host "  [5/7] Running chaos scenario..." -ForegroundColor Red
Write-Host "    FillDisk node 1..." -ForegroundColor Gray
& "$ScriptDir/FillDisk.ps1" 1 | Out-Null
Start-Sleep -Seconds 3

Write-Host "    KillNode 1..." -ForegroundColor Gray
& "$ScriptDir/KillNode.ps1" 1 | Out-Null
Start-Sleep -Seconds 5

Write-Host "    ClearDisk node 1..." -ForegroundColor Gray
& "$ScriptDir/ClearDisk.ps1" 1 | Out-Null
Start-Sleep -Seconds 2

Write-Host "    Restart node 1..." -ForegroundColor Gray
docker start chaos-rabbitmq-1 | Out-Null
Start-Sleep -Seconds 3

$preRecoveryCount = (Select-String -Path $SubscriberLog -Pattern "Order #" -ErrorAction SilentlyContinue).Count
Write-Host "    Messages before recovery wait: $preRecoveryCount" -ForegroundColor Gray

# Step 6: Wait for recovery
Write-Host "  [6/7] Waiting ${WaitAfterRecoverySeconds}s for recovery..." -ForegroundColor Yellow
Start-Sleep -Seconds $WaitAfterRecoverySeconds

$postRecoveryCount = (Select-String -Path $SubscriberLog -Pattern "Order #" -ErrorAction SilentlyContinue).Count
$newMessages = $postRecoveryCount - $preRecoveryCount

# Step 7: Report results
Write-Host "  [7/7] Results:" -ForegroundColor Cyan
Write-Host "    Baseline messages: $baselineCount"
Write-Host "    Post-chaos messages: $preRecoveryCount"
Write-Host "    New messages after recovery: $newMessages"
Write-Host ""

if ($newMessages -gt 0) {
    Write-Host "  PASS: Subscriber recovered and received $newMessages new messages" -ForegroundColor Green
} else {
    Write-Host "  FAIL: Subscriber did NOT receive any new messages after recovery" -ForegroundColor Red
    Write-Host "  This confirms the bug -- subscriber connection reports recovery but consumer is dead" -ForegroundColor Yellow
}

# Cleanup
Write-Host "`n  Cleaning up processes..." -ForegroundColor Gray
Stop-Process -Id $subscriberProcess.Id -Force -ErrorAction SilentlyContinue
Stop-Process -Id $publisherProcess.Id -Force -ErrorAction SilentlyContinue

Write-Host "  Logs saved to:"
Write-Host "    $SubscriberLog"
Write-Host "    $PublisherLog"
Write-Host ""

exit ($newMessages -gt 0 ? 0 : 1)
