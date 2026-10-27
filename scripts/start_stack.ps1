# Start Kafka (KRaft), Schema Registry, and Flink as native Windows processes.
# Stop with scripts\stop_stack.ps1. State survives restarts; wipe with stop_stack.ps1 -wipe.
#
# KRaft formatting note: Kafka 3.8 needs the storage dir formatted once before first start.

$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "runtimes.env.ps1")

$STATE_DIR = Join-Path $PSScriptRoot ".." | Join-Path -ChildPath ".state"
New-Item -ItemType Directory -Force $STATE_DIR | Out-Null
$log = Join-Path $STATE_DIR "logs"
New-Item -ItemType Directory -Force $log | Out-Null

function Test-Port {
    param([int]$Port)
    (Test-NetConnection -ComputerName localhost -Port $Port -WarningAction SilentlyContinue).TcpTestSucceeded
}

# ---- 1. Kafka ----
if (Test-Port 9092) {
    Write-Host "kafka: already running (9092 open)"
} else {
    $kafkaData = Join-Path $STATE_DIR "kafka-logs"
    if (-not (Test-Path (Join-Path $kafkaData "meta.properties"))) {
        Write-Host "kafka: formatting KRaft storage (first run)"
        $clusterUuid = & (Join-Path $KAFKA_HOME "bin\windows\kafka-storage.bat") random-uuid
        & (Join-Path $KAFKA_HOME "bin\windows\kafka-storage.bat") format `
            --cluster-id $clusterUuid `
            --config (Join-Path $KAFKA_HOME "config\kraft\server.properties") `
            --override process.roles=broker,controller `
            --override node.id=1 `
            --override controller.quorum.voters=1@localhost:9093 `
            --override listeners=PLAINTEXT://:9092,CONTROLLER://:9093 `
            --override advertised.listeners=PLAINTEXT://localhost:9092 `
            --override listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT `
            --override controller.listener.names=CONTROLLER `
            --override log.dirs="$kafkaData" `
            --override auto.create.topics.enable=false
    }
    Write-Host "kafka: starting"
    Start-Process -FilePath (Join-Path $KAFKA_HOME "bin\windows\kafka-server-start.bat") `
        -ArgumentList (Join-Path $KAFKA_HOME "config\kraft\server.properties") `
        -RedirectStandardOutput (Join-Path $log "kafka.out.log") `
        -RedirectStandardError (Join-Path $log "kafka.err.log") `
        -WindowStyle Hidden
    # wait for 9092
    for ($i = 0; $i -lt 30; $i++) {
        if (Test-Port 9092) { break }
        Start-Sleep -Seconds 2
    }
    if (-not (Test-Port 9092)) { throw "kafka did not come up on 9092; see $log\kafka.err.log" }
    Write-Host "kafka: up"
}

# ---- 2. Schema Registry ----
if (Test-Port 8081) {
    Write-Host "schema-registry: already running (8081 open)"
} else {
    Write-Host "schema-registry: starting"
    $srProps = Join-Path $STATE_DIR "schema-registry.properties"
    @"
listeners=http://0.0.0.0:8081
host.name=localhost
kafkastore.bootstrap.servers=PLAINTEXT://localhost:9092
kafkastore.topic=_schemas
schema.compatibility.level=backward
"@ | Out-File -FilePath $srProps -Encoding ascii
    Start-Process -FilePath (Join-Path $SR_HOME "bin\windows\schema-registry-start.bat") `
        -ArgumentList $srProps `
        -RedirectStandardOutput (Join-Path $log "schema-registry.out.log") `
        -RedirectStandardError (Join-Path $log "schema-registry.err.log") `
        -WindowStyle Hidden
    for ($i = 0; $i -lt 45; $i++) {
        if (Test-Port 8081) { break }
        Start-Sleep -Seconds 2
    }
    if (-not (Test-Port 8081)) { throw "schema-registry did not come up on 8081; see $log\schema-registry.err.log" }
    Write-Host "schema-registry: up"
}

# ---- 3. Flink standalone cluster ----
if (Test-Port 8082) {
    Write-Host "flink: already running (8082 open)"
} else {
    Write-Host "flink: starting local cluster (UI on 8082)"
    # Flink 1.19 on Windows: start-cluster.bat spawns JVMs for jobmanager + taskmanager
    Start-Process -FilePath (Join-Path $FLINK_HOME "bin\start-cluster.bat") `
        -WindowStyle Hidden `
        -WorkingDirectory $FLINK_HOME
    for ($i = 0; $i -lt 30; $i++) {
        if (Test-Port 8082) { break }
        Start-Sleep -Seconds 2
    }
    if (-not (Test-Port 8082)) { throw "flink UI did not come up on 8082; see $FLINK_HOME\log" }
    Write-Host "flink: up (UI: http://localhost:8082)"
}

Write-Host ""
Write-Host "stack is up. next: python producer\setup_topics.py"
