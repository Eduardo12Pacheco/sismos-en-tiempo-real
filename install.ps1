<#
.SYNOPSIS
    realtime-seismic-stream installer for Windows (Docker Desktop, Linux containers).

.DESCRIPTION
    Checks the local prerequisites, builds the Compose stack, starts the services
    and launches the offline replay. The script is idempotent: running it again
    rebuilds only what changed and never removes data volumes.
#>
[CmdletBinding()]
param(
    [switch]$Check,
    [switch]$Json,
    [switch]$Replay,
    [string]$Speed = '',
    [switch]$Chaos,
    [switch]$Down,
    [switch]$Purge,
    [switch]$Yes,
    [switch]$Help
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$ProjectName = 'realtime-seismic-stream'
$GrafanaPort = 13000
$MinComposeVersion = [version]'2.20'
$MinMemoryBytes = 4GB
$MinDiskBytes = 5GB
$SampleSpanSeconds = 604800

$script:Checks = New-Object System.Collections.ArrayList
$script:AllOk = $true

# Bind switches once at script scope; PSScriptAnalyzer's unused-parameter rule
# does not follow references made from inside functions.
$JsonOutput = [bool]$Json
$ForegroundReplay = [bool]$Replay
$ChaosEnabled = [bool]$Chaos
$AssumeYes = [bool]$Yes

$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location -LiteralPath $Root

function Show-Usage {
    Write-Output @'
realtime-seismic-stream installer (Windows)

Usage: .\install.ps1 [options]

Options:
  -Check          Check prerequisites only and exit (0 pass/warn, 1 failure)
  -Json           With -Check, print machine-readable JSON
  -Replay         Run the replay in the foreground after starting the stack
  -Speed VALUE    Replay compression factor (> 0) or "max" (no pacing)
  -Chaos          Inject malformed records, duplicates and revisions
  -Down           Stop the stack and remove orphan containers (keep volumes)
  -Purge          Remove containers, volumes, local images and .env
  -Yes            Do not ask for confirmation (only meaningful with -Purge)
  -Help           Show this help and exit

Environment:
  SEISMIC_REPLAY_SPEED   Default replay speed (1440 = one day per minute)
  SEISMIC_REPLAY_CHAOS   true/false, enables deterministic chaos injection
  SEISMIC_CHAOS_SEED     Chaos seed (default 42)

Examples:
  .\install.ps1
  .\install.ps1 -Replay -Speed 3600 -Chaos
  .\install.ps1 -Check -Json
  .\install.ps1 -Down
'@
}

function Add-CheckResult {
    param(
        [string]$Id,
        [string]$Status,
        [string]$Detail
    )
    $entry = [pscustomobject]@{ id = $Id; status = $Status; detail = $Detail }
    [void]$script:Checks.Add($entry)
}

function Invoke-Docker {
    param([string[]]$Arguments)
    $output = & docker @Arguments 2>$null
    return [pscustomobject]@{
        ExitCode = $LASTEXITCODE
        Output   = ($output -join "`n")
    }
}

function Test-SpeedValue {
    param([string]$Value)
    if ($Value -eq 'max') {
        return $true
    }
    $parsed = 0.0
    if (-not [double]::TryParse($Value, [ref]$parsed)) {
        return $false
    }
    return $parsed -gt 0
}

function Test-ShellPrerequisite {
    $current = $PSVersionTable.PSVersion
    if ($current -ge [version]'5.1') {
        Add-CheckResult -Id 'shell' -Status 'pass' -Detail "PowerShell $current"
    }
    else {
        Add-CheckResult -Id 'shell' -Status 'fail' -Detail "PowerShell $current; 5.1 or newer is required"
    }
}

function Test-DockerCliPrerequisite {
    $command = Get-Command docker -ErrorAction SilentlyContinue
    if ($null -eq $command) {
        Add-CheckResult -Id 'docker_cli' -Status 'fail' -Detail 'docker executable not found on PATH'
        return
    }
    $result = Invoke-Docker -Arguments @('--version')
    if ($result.ExitCode -ne 0 -or [string]::IsNullOrWhiteSpace($result.Output)) {
        Add-CheckResult -Id 'docker_cli' -Status 'fail' -Detail "docker is installed but 'docker --version' failed"
        return
    }
    Add-CheckResult -Id 'docker_cli' -Status 'pass' -Detail $result.Output.Trim()
}

function Test-DockerComposePrerequisite {
    if ($null -eq (Get-Command docker -ErrorAction SilentlyContinue)) {
        Add-CheckResult -Id 'docker_compose' -Status 'fail' -Detail 'docker executable not found on PATH'
        return
    }
    $result = Invoke-Docker -Arguments @('compose', 'version', '--short')
    if ($result.ExitCode -ne 0) {
        $result = Invoke-Docker -Arguments @('compose', 'version')
    }
    $match = [regex]::Match($result.Output, '\d+\.\d+(\.\d+)?')
    if ($result.ExitCode -ne 0 -or -not $match.Success) {
        Add-CheckResult -Id 'docker_compose' -Status 'fail' -Detail 'Docker Compose v2 is required but was not found'
        return
    }
    $versionText = $match.Value
    $parts = $versionText.Split('.')
    $version = [version]($parts[0] + '.' + $parts[1])
    if ($version -lt $MinComposeVersion) {
        Add-CheckResult -Id 'docker_compose' -Status 'fail' -Detail "Docker Compose $versionText found; >= 2.20 is required"
        return
    }
    Add-CheckResult -Id 'docker_compose' -Status 'pass' -Detail "Docker Compose $versionText"
}

function Test-DockerEnginePrerequisite {
    if ($null -eq (Get-Command docker -ErrorAction SilentlyContinue)) {
        Add-CheckResult -Id 'docker_engine' -Status 'fail' -Detail 'docker executable not found on PATH'
        return
    }
    $result = Invoke-Docker -Arguments @('info', '--format', '{{.OSType}}')
    $osType = $result.Output.Trim()
    if ($result.ExitCode -ne 0 -or [string]::IsNullOrWhiteSpace($osType)) {
        Add-CheckResult -Id 'docker_engine' -Status 'fail' -Detail 'Docker daemon unreachable; start Docker Desktop and try again'
        return
    }
    if ($osType -ne 'linux') {
        Add-CheckResult -Id 'docker_engine' -Status 'fail' -Detail "Docker engine uses '$osType' containers; Linux containers are required"
        return
    }
    Add-CheckResult -Id 'docker_engine' -Status 'pass' -Detail 'Docker engine reachable with Linux containers'
}

function Test-MemoryPrerequisite {
    $bytes = $null
    if ($env:OS -eq 'Windows_NT') {
        $computer = Get-CimInstance -ClassName Win32_ComputerSystem -ErrorAction SilentlyContinue
        if ($null -ne $computer -and $null -ne $computer.TotalPhysicalMemory) {
            $bytes = [double]$computer.TotalPhysicalMemory
        }
    }
    elseif (Test-Path -LiteralPath '/proc/meminfo') {
        $line = Get-Content -LiteralPath '/proc/meminfo' |
            Where-Object { $_ -match '^MemTotal:\s+(\d+)' } |
            Select-Object -First 1
        if ($null -ne $line -and $line -match '^MemTotal:\s+(\d+)') {
            $bytes = [double]$Matches[1] * 1KB
        }
    }
    else {
        $sysctl = Get-Command sysctl -ErrorAction SilentlyContinue
        if ($null -ne $sysctl) {
            $raw = & sysctl -n hw.memsize 2>$null
            if ($LASTEXITCODE -eq 0 -and $null -ne $raw -and "$raw" -match '^\d+$') {
                $bytes = [double]$raw
            }
        }
    }
    if ($null -eq $bytes) {
        Add-CheckResult -Id 'memory' -Status 'warn' -Detail 'unable to determine total memory'
        return
    }
    $gib = [math]::Round($bytes / 1GB, 1)
    if ($bytes -lt $MinMemoryBytes) {
        Add-CheckResult -Id 'memory' -Status 'warn' -Detail "$gib GiB detected; 4 GiB or more is recommended"
        return
    }
    Add-CheckResult -Id 'memory' -Status 'pass' -Detail "$gib GiB total memory"
}

function Test-DiskPrerequisite {
    try {
        $drive = [System.IO.DriveInfo]::new((Get-Location -PSProvider FileSystem).Path)
        $available = [double]$drive.AvailableFreeSpace
    }
    catch {
        Add-CheckResult -Id 'disk' -Status 'warn' -Detail 'unable to determine free disk space'
        return
    }
    $gib = [math]::Round($available / 1GB, 1)
    if ($available -lt $MinDiskBytes) {
        Add-CheckResult -Id 'disk' -Status 'warn' -Detail "$gib GiB free; 5 GiB or more is recommended"
        return
    }
    Add-CheckResult -Id 'disk' -Status 'pass' -Detail "$gib GiB free"
}

function Test-PortOpen {
    $client = New-Object System.Net.Sockets.TcpClient
    try {
        $connection = $client.BeginConnect('127.0.0.1', $GrafanaPort, $null, $null)
        if (-not $connection.AsyncWaitHandle.WaitOne(1000, $false)) {
            return $false
        }
        $client.EndConnect($connection)
        return $true
    }
    catch {
        return $false
    }
    finally {
        $client.Close()
    }
}

function Test-PortPrerequisite {
    $owners = @()
    if ($null -ne (Get-Command docker -ErrorAction SilentlyContinue)) {
        $result = Invoke-Docker -Arguments @(
            'ps',
            '--filter',
            "publish=$GrafanaPort",
            '--format',
            '{{.Label "com.docker.compose.project"}}'
        )
        if ($result.ExitCode -eq 0) {
            $owners = @($result.Output -split "`r?`n" | Where-Object { $_ -ne '' })
        }
    }
    if ($owners -contains $ProjectName) {
        Add-CheckResult -Id 'port_13000' -Status 'pass' -Detail "port $GrafanaPort is published by this project's Grafana"
        return
    }
    if ($owners.Count -gt 0) {
        Add-CheckResult -Id 'port_13000' -Status 'fail' -Detail "port $GrafanaPort is published by another Compose project ('$($owners[0])')"
        return
    }
    if (Test-PortOpen) {
        Add-CheckResult -Id 'port_13000' -Status 'fail' -Detail "port $GrafanaPort is already in use by a non-Compose process"
        return
    }
    Add-CheckResult -Id 'port_13000' -Status 'pass' -Detail "port $GrafanaPort is free"
}

function Write-CheckOutput {
    param([bool]$Ok)
    if ($JsonOutput) {
        $payload = [ordered]@{
            checks = @($script:Checks)
            ok     = $Ok
        }
        Write-Output ($payload | ConvertTo-Json -Depth 5 -Compress)
        return
    }
    Write-Output 'Prerequisite checks:'
    foreach ($item in $script:Checks) {
        $label = switch ($item.status) {
            'pass' { '[PASS]' }
            'warn' { '[WARN]' }
            default { '[FAIL]' }
        }
        Write-Output ('  {0,-6} {1,-15} {2}' -f $label, $item.id, $item.detail)
    }
    if ($Ok) {
        Write-Output 'All prerequisites satisfied (warnings do not block the install).'
    }
    else {
        Write-Output 'One or more prerequisites failed; fix them and run .\install.ps1 -Check again.'
    }
}

function Invoke-CheckSuite {
    $script:Checks.Clear()
    Test-ShellPrerequisite
    Test-DockerCliPrerequisite
    Test-DockerComposePrerequisite
    Test-DockerEnginePrerequisite
    Test-MemoryPrerequisite
    Test-DiskPrerequisite
    Test-PortPrerequisite

    $script:AllOk = $true
    foreach ($item in $script:Checks) {
        if ($item.status -eq 'fail') {
            $script:AllOk = $false
        }
    }
    Write-CheckOutput -Ok $script:AllOk
}

function Copy-EnvFile {
    if (-not (Test-Path -LiteralPath '.env')) {
        Copy-Item -LiteralPath '.env.example' -Destination '.env'
        Write-Output 'Created .env from .env.example (edit it to change the defaults).'
    }
}

function Invoke-Compose {
    param([string[]]$Arguments)
    & docker compose @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "docker compose $($Arguments -join ' ') failed with exit code $LASTEXITCODE"
    }
}

function Get-EffectiveSpeed {
    if ($Speed -ne '') {
        return $Speed
    }
    if (-not [string]::IsNullOrWhiteSpace($env:SEISMIC_REPLAY_SPEED)) {
        return $env:SEISMIC_REPLAY_SPEED
    }
    return '1440'
}

function Get-ReplayEstimate {
    param([string]$Value)
    if ($Value -eq 'max') {
        return 'instant (pacing disabled)'
    }
    $parsed = 1440.0
    [void][double]::TryParse($Value, [ref]$parsed)
    if ($parsed -le 0) {
        $parsed = 1440.0
    }
    $minutes = ($SampleSpanSeconds / $parsed) / 60
    return ('about {0:N1} minutes of wall time' -f $minutes)
}

function Invoke-StackDown {
    Write-Output 'Stopping the stack (volumes are kept)...'
    Invoke-Compose -Arguments @('down', '--remove-orphans')
    Write-Output 'Stack stopped.'
}

function Invoke-StackPurge {
    if (-not $AssumeYes) {
        $reply = Read-Host "This removes every container, volume, local image and .env for $ProjectName. Continue? [y/N]"
        if ($reply -notmatch '^y(es)?$') {
            Write-Output 'Purge cancelled.'
            exit 1
        }
    }
    Write-Output 'Purging containers, volumes, local images and .env...'
    try {
        Invoke-Compose -Arguments @('down', '-v', '--remove-orphans', '--rmi', 'local')
    }
    catch {
        Write-Output $_.Exception.Message
    }
    if (Test-Path -LiteralPath '.env') {
        Remove-Item -LiteralPath '.env' -Force
    }
    Write-Output 'Purge complete.'
}

function Invoke-Install {
    Copy-EnvFile
    Write-Output 'Building the application images (this can take a few minutes)...'
    Invoke-Compose -Arguments @('build')
    Write-Output 'Starting Redpanda, ClickHouse, Prometheus, Grafana, processor, sink and monitor...'
    Invoke-Compose -Arguments @(
        'up', '-d', '--wait',
        'redpanda', 'clickhouse', 'prometheus', 'grafana', 'processor', 'sink', 'monitor'
    )

    $effectiveSpeed = Get-EffectiveSpeed
    if ($ForegroundReplay) {
        $env:SEISMIC_REPLAY_SPEED = $effectiveSpeed
        if ($ChaosEnabled) {
            $env:SEISMIC_REPLAY_CHAOS = 'true'
        }
        Write-Output "Running the replay in the foreground (speed $effectiveSpeed)..."
        Invoke-Compose -Arguments @('run', '--rm', 'ingest')
    }
    else {
        if ($Speed -ne '') {
            $env:SEISMIC_REPLAY_SPEED = $Speed
        }
        if ($ChaosEnabled) {
            $env:SEISMIC_REPLAY_CHAOS = 'true'
        }
        Write-Output "Starting the replay (speed $effectiveSpeed)..."
        Invoke-Compose -Arguments @('up', '-d', 'ingest')
    }

    Write-Output ''
    Write-Output "Grafana: http://localhost:$GrafanaPort"
    Write-Output "Replay: speed $effectiveSpeed ($(Get-ReplayEstimate -Value $effectiveSpeed))"
    Write-Output 'Commands: .\install.ps1 -Down | .\install.ps1 -Purge -Yes | .\install.ps1 -Help'
}

if ($Help) {
    Show-Usage
    exit 0
}

if ($Speed -ne '' -and -not (Test-SpeedValue -Value $Speed)) {
    [Console]::Error.WriteLine('error: -Speed must be a positive number or "max"')
    Show-Usage
    exit 2
}

if ($Check) {
    Invoke-CheckSuite
    if ($script:AllOk) {
        exit 0
    }
    exit 1
}

if ($Purge) {
    Invoke-StackPurge
    exit 0
}

if ($Down) {
    Invoke-StackDown
    exit 0
}

Invoke-CheckSuite
if (-not $script:AllOk) {
    exit 1
}

Invoke-Install
