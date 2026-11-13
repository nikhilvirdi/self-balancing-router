<#
.SYNOPSIS
    Executes a scenario-driven k6 benchmark workload for self-balancing-router.

.DESCRIPTION
    Runs the Grafana k6 workload generator against self-balancing-router proxy.
    Automatically detects whether a local k6 binary is installed or falls back
    to running grafana/k6 via Docker.

.PARAMETER Scenario
    Scenario ID, index (1-7), or filename (default: "scenario_1_homogeneous").

.PARAMETER ProxyUrl
    Base URL of the running proxy service (default: "http://127.0.0.1:8080").

.PARAMETER Policy
    Routing policy name to tag in metric outputs (e.g. "round-robin", "p2c").

.PARAMETER Output
    Path to save machine-readable JSON summary results.

.PARAMETER HttpTimeout
    Timeout for individual HTTP requests (default: "10s").

.PARAMETER PreallocatedVUs
    Override for preallocated virtual users.

.PARAMETER MaxVUs
    Override for maximum virtual users.

.PARAMETER UseDocker
    Forces execution via Docker container even if local k6 is available.

.EXAMPLE
    .\experiments\k6\run.ps1 -Scenario scenario_1_homogeneous -Policy round-robin -Output experiments/results/scenario_1.json
#>

[CmdletBinding()]
param(
    [Parameter(Position = 0)]
    [string]$Scenario = "scenario_1_homogeneous",

    [Parameter()]
    [string]$ProxyUrl = "http://127.0.0.1:8080",

    [Parameter()]
    [string]$Policy = "unspecified",

    [Parameter()]
    [string]$Output = "",

    [Parameter()]
    [string]$HttpTimeout = "10s",

    [Parameter()]
    [int]$PreallocatedVUs = 0,

    [Parameter()]
    [int]$MaxVUs = 0,

    [Parameter()]
    [switch]$UseDocker
)

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Definition
$repoRoot = Split-Path -Parent (Split-Path -Parent $scriptDir)
$workloadScript = "experiments/k6/workload.js"

# Check if local k6 is available
$hasLocalK6 = $false
if (-not $UseDocker) {
    try {
        $null = Get-Command "k6" -ErrorAction Stop
        $hasLocalK6 = $true
    } catch {
        $hasLocalK6 = $false
    }
}

# Ensure results directory exists if output path is specified
if ($Output -ne "") {
    $outDir = Split-Path -Parent $Output
    if ($outDir -ne "" -and -not (Test-Path $outDir)) {
        New-Item -ItemType Directory -Force -Path $outDir | Out-Null
    }
}

if ($hasLocalK6) {
    Write-Host "[k6 runner] Using local k6 binary..." -ForegroundColor Cyan

    $envArgs = @(
        "-e", "SBR_SCENARIO=$Scenario",
        "-e", "SBR_PROXY_URL=$ProxyUrl",
        "-e", "SBR_PROXY_POLICY=$Policy",
        "-e", "SBR_HTTP_TIMEOUT=$HttpTimeout"
    )

    if ($Output -ne "") {
        $envArgs += @("-e", "SBR_SUMMARY_OUTPUT=$Output")
    }
    if ($PreallocatedVUs -gt 0) {
        $envArgs += @("-e", "SBR_VUS_PREALLOCATED=$PreallocatedVUs")
    }
    if ($MaxVUs -gt 0) {
        $envArgs += @("-e", "SBR_VUS_MAX=$MaxVUs")
    }

    $allArgs = @("run") + $envArgs + @($workloadScript)
    & k6 @allArgs
    exit $LASTEXITCODE
} else {
    Write-Host "[k6 runner] Local k6 not found; executing via Docker grafana/k6:latest..." -ForegroundColor Cyan

    # For Docker container, 127.0.0.1 on Windows host is accessed via host.docker.internal
    $dockerProxyUrl = $ProxyUrl
    if ($dockerProxyUrl -match "127\.0\.0\.1" -or $dockerProxyUrl -match "localhost") {
        $dockerProxyUrl = $dockerProxyUrl -replace "127\.0\.0\.1", "host.docker.internal" -replace "localhost", "host.docker.internal"
    }

    $dockerEnvArgs = @(
        "-e", "SBR_SCENARIO=$Scenario",
        "-e", "SBR_PROXY_URL=$dockerProxyUrl",
        "-e", "SBR_PROXY_POLICY=$Policy",
        "-e", "SBR_HTTP_TIMEOUT=$HttpTimeout"
    )

    if ($Output -ne "") {
        $dockerEnvArgs += @("-e", "SBR_SUMMARY_OUTPUT=/workspace/$Output")
    }
    if ($PreallocatedVUs -gt 0) {
        $dockerEnvArgs += @("-e", "SBR_VUS_PREALLOCATED=$PreallocatedVUs")
    }
    if ($MaxVUs -gt 0) {
        $dockerEnvArgs += @("-e", "SBR_VUS_MAX=$MaxVUs")
    }

    $dockerArgs = @(
        "run", "--rm",
        "-v", "${repoRoot}:/workspace",
        "-w", "/workspace"
    ) + $dockerEnvArgs + @(
        "grafana/k6:latest",
        "run",
        "/workspace/$workloadScript"
    )

    & docker @dockerArgs
    exit $LASTEXITCODE
}

