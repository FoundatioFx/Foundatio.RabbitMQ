# Reset cluster - removes everything
Push-Location $PSScriptRoot/..

Write-Host "`n  Resetting cluster..." -ForegroundColor Yellow
docker compose down -v --remove-orphans 2>&1 | Out-Null
Write-Host "  Done. Run ./scripts/Start.ps1 to start fresh." -ForegroundColor Green
Write-Host ""

Pop-Location
