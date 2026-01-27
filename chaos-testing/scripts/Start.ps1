# Start the RabbitMQ chaos testing cluster
Push-Location $PSScriptRoot/..

Write-Host "`n  Starting RabbitMQ chaos cluster..." -ForegroundColor Cyan
docker compose up ready 2>&1 | Out-Null

Write-Host "  Cluster ready!" -ForegroundColor Green
Write-Host "`n  Connect: " -NoNewline
Write-Host "amqp://guest:guest@localhost:5672" -ForegroundColor Yellow
Write-Host "`n  Management UIs:" -ForegroundColor Gray
Write-Host "    Node 1 (50MB):  http://localhost:15672"
Write-Host "    Node 2 (75MB):  http://localhost:15673"
Write-Host "    Node 3 (100MB): http://localhost:15674"
Write-Host ""

Pop-Location
