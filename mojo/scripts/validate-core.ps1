param(
    [switch]$ListOnly
)

$ErrorActionPreference = "Stop"

$rootDir = Split-Path -Parent $PSScriptRoot
$coreDir = Join-Path $rootDir "neutron-mojo"
$testDir = Join-Path $coreDir "test"
$reportDir = Join-Path $rootDir "reports"

New-Item -ItemType Directory -Path $reportDir -Force | Out-Null

function Resolve-MojoBin {
    if ($env:MOJO_BIN -and (Test-Path $env:MOJO_BIN)) {
        return $env:MOJO_BIN
    }

    $cmd = Get-Command mojo -ErrorAction SilentlyContinue
    if ($cmd) {
        return $cmd.Source
    }

    if (-not $IsWindows) {
        $pixiMojoUnix = Join-Path $coreDir ".pixi\envs\default\bin\mojo"
        if (Test-Path $pixiMojoUnix) {
            return $pixiMojoUnix
        }
    }

    $pixiMojoExe = Join-Path $coreDir ".pixi\envs\default\bin\mojo.exe"
    if (Test-Path $pixiMojoExe) {
        return $pixiMojoExe
    }

    if ($IsWindows) {
        throw "Mojo executable not found for native Windows execution. Use WSL + bash validator, or set MOJO_BIN to a Windows-compatible mojo executable."
    }
    throw "Mojo executable not found. Set MOJO_BIN or install the neutron-mojo pixi environment."
}

$tests = Get-ChildItem -Path $testDir -Filter "test_*.mojo" -File | Sort-Object Name
if ($tests.Count -eq 0) {
    throw "No core tests found at $testDir"
}

$timestamp = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
$runId = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ")
$summaryFile = Join-Path $reportDir "core-validation-latest.md"
$runSummaryFile = Join-Path $reportDir "core-validation-$runId.md"
$csvFile = Join-Path $reportDir "core-validation-$runId.csv"
$logFile = Join-Path $reportDir "core-validation-$runId.log"
$mojoBin = "(not found)"
try {
    $mojoBin = Resolve-MojoBin
}
catch {
    if (-not $ListOnly) {
        throw
    }
}

if ($ListOnly) {
    $lines = @()
    $lines += "# Mojo Core Validation (List Only)"
    $lines += ""
    $lines += "- Timestamp (UTC): $timestamp"
    $lines += "- Mojo binary: ``$mojoBin``"
    $lines += "- Core test files discovered: $($tests.Count)"
    $lines += "- Execution: not run (``--list-only``)"
    $lines += ""
    $lines += "## Tests"
    foreach ($test in $tests) {
        $lines += "- ``$($test.Name)``"
    }
    $lines | Set-Content -Path $summaryFile
    Copy-Item -Path $summaryFile -Destination $runSummaryFile -Force
    Write-Output "Wrote list-only summary: $summaryFile"
    exit 0
}

if ($mojoBin -eq "(not found)") {
    throw "Mojo executable not found. Set MOJO_BIN or run from an environment where mojo is executable."
}

# If using a direct pixi Mojo binary, set MAX activation variables explicitly.
if ($mojoBin -match "[/\\]\.pixi[/\\]envs[/\\]default[/\\]bin[/\\]mojo(\.exe)?$") {
    $envPrefix = Split-Path -Parent (Split-Path -Parent $mojoBin)
    $env:CONDA_PREFIX = $envPrefix
    $env:MODULAR_HOME = Join-Path $envPrefix "share/max"
}

"test,status,exit_code" | Set-Content -Path $csvFile

$logHeader = @(
    "Mojo core validation run",
    "timestamp=$timestamp",
    "mojo_bin=$mojoBin",
    ""
)
$logHeader | Set-Content -Path $logFile

$passCount = 0
$failCount = 0
$skipHintCount = 0

foreach ($test in $tests) {
    $testName = $test.Name
    ">>> Running $testName" | Tee-Object -FilePath $logFile -Append | Out-Null

    Push-Location $coreDir
    try {
        $output = & $mojoBin run -I src ("test/" + $testName) 2>&1
        $exitCode = $LASTEXITCODE
    }
    finally {
        Pop-Location
    }

    $output | Add-Content -Path $logFile
    "" | Add-Content -Path $logFile

    $status = "pass"
    if ($exitCode -ne 0) {
        $status = "fail"
        $failCount++
    }
    else {
        $passCount++
    }

    if (($output | Out-String) -match "SKIP") {
        $skipHintCount++
    }

    "$testName,$status,$exitCode" | Add-Content -Path $csvFile
}

$summaryLines = @(
    "# Mojo Core Validation",
    "",
    "- Timestamp (UTC): $timestamp",
    "- Mojo binary: ``$mojoBin``",
    "- Total tests: $($tests.Count)",
    "- Passed: $passCount",
    "- Failed: $failCount",
    "- Tests with SKIP output hints: $skipHintCount",
    "",
    "## Artifacts",
    "",
    "- CSV: ``$(Split-Path -Leaf $csvFile)``",
    "- Log: ``$(Split-Path -Leaf $logFile)``"
)

$summaryLines | Set-Content -Path $summaryFile
Copy-Item -Path $summaryFile -Destination $runSummaryFile -Force

Write-Output "Validation summary: $summaryFile"
Write-Output "Validation CSV: $csvFile"
Write-Output "Validation log: $logFile"

if ($failCount -ne 0) {
    exit 1
}
