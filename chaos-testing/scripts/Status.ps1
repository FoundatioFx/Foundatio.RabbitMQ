# Show cluster status
Push-Location $PSScriptRoot/..

Write-Host "`n  RabbitMQ Chaos Cluster Status" -ForegroundColor Cyan
Write-Host "  ==============================`n" -ForegroundColor Cyan

Write-Host "  Node    Status     Health        Disk                   Memory                  CPU" -ForegroundColor Gray
Write-Host "  ----    ------     ------        ----                   ------                  ---" -ForegroundColor Gray

foreach ($i in 1..3) {
    $container = "chaos-rabbitmq-$i"
    $info = docker ps -a --filter "name=$container" --format "{{.Status}}" 2>$null

    if ($info -match "Up") {
        $health = if ($info -match "healthy") { "healthy" } elseif ($info -match "unhealthy") { "unhealthy" } else { "starting" }

        # Disk info
        $diskInfo = docker exec $container df /var/lib/rabbitmq --output=used,size,pcent 2>$null | Select-Object -Last 1
        $diskParts = ($diskInfo.Trim() -split '\s+')
        $diskUsed = $diskParts[0]
        $diskTotal = $diskParts[1]
        $diskPct = [int]($diskParts[2] -replace '%','')
        $disk = "$diskUsed/$diskTotal"

        # Memory and CPU
        $stats = docker stats $container --no-stream --format "{{.MemUsage}},{{.MemPerc}},{{.CPUPerc}}" 2>$null
        $memUsage, $memPct, $cpuPct = $stats -split ","
        $memUsage = ($memUsage -split '/')[0].Trim()
        $memLimit = ($stats -split ',')[0] -split '/' | Select-Object -Last 1
        $memLimit = $memLimit.Trim()
        $memPctNum = [double]($memPct -replace '%','')
        $cpuPctNum = [double]($cpuPct -replace '%','')

        # Colors based on usage
        $healthColor = switch ($health) { "healthy" { "Green" } "unhealthy" { "Red" } default { "Yellow" } }
        $diskColor = if ($diskPct -ge 80) { "Red" } elseif ($diskPct -ge 50) { "Yellow" } else { "Green" }
        $memColor = if ($memPctNum -ge 80) { "Red" } elseif ($memPctNum -ge 50) { "Yellow" } else { "Green" }
        $cpuColor = if ($cpuPctNum -ge 80) { "Red" } elseif ($cpuPctNum -ge 50) { "Yellow" } else { "Green" }

        Write-Host "    $i     " -NoNewline
        Write-Host "Up".PadRight(11) -ForegroundColor Green -NoNewline
        Write-Host $health.PadRight(14) -ForegroundColor $healthColor -NoNewline
        Write-Host $disk.PadRight(14) -NoNewline
        Write-Host "$diskPct%".PadRight(9) -ForegroundColor $diskColor -NoNewline
        Write-Host "$memUsage/$memLimit".PadRight(17) -NoNewline
        Write-Host $memPct.PadRight(9) -ForegroundColor $memColor -NoNewline
        Write-Host $cpuPct -ForegroundColor $cpuColor
    } else {
        Write-Host "    $i     " -NoNewline
        Write-Host "Down".PadRight(11) -ForegroundColor Red -NoNewline
        Write-Host "-".PadRight(14) -NoNewline
        Write-Host "-".PadRight(23) -NoNewline
        Write-Host "-".PadRight(26) -NoNewline
        Write-Host "-"
    }
}

# Check for alarms on any running node
$alarmNode = $null
foreach ($i in 1..3) {
    $running = docker ps --filter "name=chaos-rabbitmq-$i" --format "{{.Names}}" 2>$null
    if ($running) { $alarmNode = $i; break }
}

if ($alarmNode) {
    $alarms = docker exec chaos-rabbitmq-$alarmNode rabbitmqctl list_alarms 2>$null
    if ($alarms -match "\[\]") {
        Write-Host "`n  Alarms:  " -NoNewline
        Write-Host "None" -ForegroundColor Green
    } else {
        Write-Host "`n  Alarms:  " -NoNewline -ForegroundColor Red
        Write-Host "$alarms" -ForegroundColor Red
    }
}

Write-Host ""
Pop-Location
