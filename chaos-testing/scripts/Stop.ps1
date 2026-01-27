# Stop the chaos testing cluster
Push-Location $PSScriptRoot/..

Write-Host "`n  Stopping cluster..." -ForegroundColor Yellow
docker compose down 2>&1 | Out-Null
Write-Host "  Done." -ForegroundColor Green
Write-Host ""

Pop-Location
