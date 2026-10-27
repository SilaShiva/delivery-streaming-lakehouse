# One-time: download Kafka, Flink, and Confluent Schema Registry for native Windows use.
# Idempotent — re-running skips anything already downloaded.
#
# Prereq: Java 17 (Temurin recommended) on PATH. Kafka 3.8 and Flink 1.19 both need it.
#
#   winget install EclipseAdoptium.Temurin.17.JDK
#
# Tarballs land in $RUNTIMES_DIR; scripts\start_stack.ps1 expects them there.

$ErrorActionPreference = "Stop"

$RUNTIMES_DIR = Join-Path $PSScriptRoot ".." | Join-Path -ChildPath "runtimes"
New-Item -ItemType Directory -Force $RUNTIMES_DIR | Out-Null

function Get-IfMissing {
    param([string]$Url, [string]$OutFile)
    $out = Join-Path $RUNTIMES_DIR $OutFile
    if (Test-Path $out) {
        Write-Host "have: $OutFile"
    } else {
        Write-Host "downloading: $OutFile"
        Invoke-WebRequest -Uri $Url -OutFile $out -UseBasicParsing
    }
    return $out
}

function Expand-Tarball {
    param([string]$Tarball)
    if (-not (Test-Path $Tarball)) { throw "missing tarball: $Tarball" }
    $dest = Join-Path $RUNTIMES_DIR ([IO.Path]::GetFileNameWithoutExtension($Tarball) -replace '\.tgz$', '')
    if (Test-Path $dest) {
        Write-Host "already extracted: $dest"
        return $dest
    }
    Write-Host "extracting: $Tarball"
    tar -xzf $Tarball -C $RUNTIMES_DIR
    return $dest
}

# --- Kafka (KRaft, no ZooKeeper) ---
$kafkaTgz = Get-IfMissing `
    -Url "https://downloads.apache.org/kafka/3.8.0/kafka_2.13-3.8.0.tgz" `
    -OutFile "kafka_2.13-3.8.0.tgz"
$kafkaHome = Expand-Tarball $kafkaTgz

# --- Flink (Windows-capable: bin\flink.bat, bin\start-cluster.bat) ---
$flinkTgz = Get-IfMissing `
    -Url "https://downloads.apache.org/flink/flink-1.19.1/flink-1.19.1-bin-scala_2.12.tgz" `
    -OutFile "flink-1.19.1-bin-scala_2.12.tgz"
$flinkHome = Expand-Tarball $flinkTgz

# --- Confluent Schema Registry (Java, runs on Windows) ---
$srZip = Get-IfMissing `
    -Url "https://packages.confluent.org/archive/community/confluent-community-7.6.1.zip" `
    -OutFile "confluent-community-7.6.1.zip"
$srDir = Join-Path $RUNTIMES_DIR "confluent-7.6.1"
if (-not (Test-Path $srDir)) {
    Write-Host "extracting: $srZip"
    Expand-Archive -Path $srZip -DestinationPath $RUNTIMES_DIR -Force
}

# --- Iceberg Flink runtime jar: Flink SQL speaks Iceberg with this on the classpath ---
Get-IfMissing `
    -Url "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.19/1.6.1/iceberg-flink-runtime-1.19-1.6.1.jar" `
    -OutFile "iceberg-flink-runtime-1.19-1.6.1.jar" | Out-Null

# Write env vars the other scripts read
@"
`$KAFKA_HOME = "$kafkaHome"
`$FLINK_HOME = "$flinkHome"
`$SR_HOME    = "$srDir"
"@ | Out-File -FilePath (Join-Path $PSScriptRoot "runtimes.env.ps1") -Encoding utf8

Write-Host ""
Write-Host "runtimes ready under $RUNTIMES_DIR"
Write-Host "next: .\scripts\start_stack.ps1"
