param(
    [switch]$Test,
    [string[]]$Modules = @("others", "rpcOrgrpc", "toUser", "toProject", "api")
)

$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot

$env:GOROOT = Join-Path $root ".gotoolchain"
$env:GOTOOLCHAIN = "local"
$env:GOCACHE = Join-Path $root ".gocache"
$env:GOTMPDIR = Join-Path $root ".gotmp"
$go = Join-Path $env:GOROOT "bin\go.exe"

if (-not (Test-Path $go)) {
    Write-Error "找不到本地 Go 工具链: $go"
}

$failed = @()

foreach ($m in $Modules) {
    $dir = Join-Path $root $m
    Write-Host "=== $m ===" -ForegroundColor Cyan
    Push-Location $dir
    try {
        & $go build ./...
        if ($LASTEXITCODE -ne 0) { $failed += "$m:build" }

        if ($Test) {
            & $go vet ./...
            if ($LASTEXITCODE -ne 0) { $failed += "$m:vet" }

            & $go test ./...
            if ($LASTEXITCODE -ne 0) { $failed += "$m:test" }
        }
    }
    finally {
        Pop-Location
    }
}

if ($failed.Count -gt 0) {
    Write-Host ("FAILED: " + ($failed -join ", ")) -ForegroundColor Red
    exit 1
}

Write-Host "all modules build ok" -ForegroundColor Green
