# Submit the Flink jobs to the local standalone cluster.
#   sql job    : real-time windows over orders (event-time, watermarks)
#   iceberg job: batch tier, orders -> Iceberg (exactly-once)

$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "runtimes.env.ps1")

$REPO = Join-Path $PSScriptRoot ".."

# Iceberg runtime + flink-connector-kafka jars must be on Flink's lib/ path.
# download_runtimes.ps1 fetches the iceberg jar; the kafka connector:
$kafkaConnector = Get-ChildItem $env:USERPROFILE ".m2\repository\org\apache\flink\flink-connector-kafka" -Recurse -Filter "*.jar" -ErrorAction SilentlyContinue |
    Select-Object -First 1 -ExpandProperty FullName
if (-not $kafkaConnector) {
    Write-Warning "flink-connector-kafka jar not found in ~/.m2 — download it once:"
    Write-Host "  https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/3.2.0-1.19/flink-connector-kafka-3.2.0-1.19.jar"
    Write-Host "  -> copy into $FLINK_HOME\lib\ and restart the cluster"
} elseif (Test-Path (Join-Path $FLINK_HOME "lib\flink-connector-kafka-3.2.0-1.19.jar")) {
    Write-Host "flink lib: kafka connector already in place"
} else {
    Copy-Item $kafkaConnector (Join-Path $FLINK_HOME "lib\") -Force
    Write-Host "flink lib: copied kafka connector; RESTART the cluster for it to load"
}

$icebergJar = Join-Path (Join-Path $PSScriptRoot ".." | Join-Path -ChildPath "runtimes") "iceberg-flink-runtime-1.19-1.6.1.jar"
if ((Test-Path $icebergJar) -and -not (Test-Path (Join-Path $FLINK_HOME "lib\iceberg-flink-runtime-1.19-1.6.1.jar"))) {
    Copy-Item $icebergJar (Join-Path $FLINK_HOME "lib\") -Force
    Write-Host "flink lib: copied iceberg runtime; RESTART the cluster for it to load"
}

# SQL job: file-based submission (Flink SQL scripts run via sql-client.sh on Linux;
# on Windows the SQL client is Linux-only, so we submit through the gateway below).
Write-Host ""
Write-Host "Note: Flink's SQL client is Linux-only upstream. The SQL job is submitted via"
Write-Host "the embedded gateway in flink\iceberg_sink\submit_sql_windows.py (Py4J to the"
Write-Host "running cluster's TableEnvironment gateway). See docs/decisions.md D11."
Write-Host ""
Write-Host "DataStream walkthrough job (optional, interview reference):"
Write-Host "  flink run -py flink\pyflink_walkthrough\realtime_metrics.py"
