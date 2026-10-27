# Stop the native Windows stack. -wipe also deletes Kafka + Flink + Iceberg state.

param([switch]$wipe)

$ErrorActionPreference = "Continue"
. (Join-Path $PSScriptRoot "runtimes.env.ps1")

$STATE_DIR = Join-Path $PSScriptRoot ".." | Join-Path -ChildPath ".state"

# Flink first (it holds consumers against Kafka)
if (Test-Path (Join-Path $FLINK_HOME "bin\stop-cluster.bat")) {
    Write-Host "flink: stopping cluster"
    & (Join-Path $FLINK_HOME "bin\stop-cluster.bat") 2>$null
}

# Schema Registry
Get-Process -Name java -ErrorAction SilentlyContinue |
    Where-Object { $_.CommandLine -match "schema-registry" } |
    Stop-Process -Force -ErrorAction SilentlyContinue

# Kafka
Get-Process -Name java -ErrorAction SilentlyContinue |
    Where-Object { $_.CommandLine -match "kafka.Kafka" } |
    Stop-Process -Force -ErrorAction SilentlyContinue

Write-Host "stack stopped."

if ($wipe) {
    if (Test-Path $STATE_DIR) { Remove-Item $STATE_DIR -Recurse -Force }
    Write-Host "state wiped (kafka logs, flink state, duckdb warehouse)."
}
