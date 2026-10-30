# Start Kafka (KRaft), Schema Registry, and Flink as native Windows processes.
# Stop with scripts\stop_stack.ps1. State survives restarts; wipe with stop_stack.ps1 -wipe.
#
# Windows notes (D11):
#   * Kafka 4.x: broker and CLI tools are launched via java directly with a
#     wildcard classpath — the shipped .bat scripts expand all 109 jars into
#     the CLASSPATH and cmd.exe rejects the >32KB command line.
#   * Flink 2.x dropped its .bat scripts; jobmanager/taskmanager are launched
#     via scripts\flink_jm.cmd / flink_tm.cmd (direct JVM entrypoints).
#   * Schema Registry ships bash scripts only; launched via its Main class.
#   * TaskManager needs taskmanager.resource-id set (colon-free) — the default
#     id is "host:port-hash" and colon is illegal in Windows NTFS paths.

$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "runtimes.env.ps1")

$env:JAVA_HOME = $JAVA_HOME
$env:PATH = "$JAVA_HOME\bin;$env:PATH"

$REPO = (Get-Item (Join-Path $PSScriptRoot "..")).FullName
$STATE_DIR = Join-Path $REPO ".state"
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
    $serverProps = Join-Path $STATE_DIR "kafka-server.properties"
    if (-not (Test-Path $serverProps)) {
        @"
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:9093
listeners=PLAINTEXT://:9092,CONTROLLER://:9093
advertised.listeners=PLAINTEXT://localhost:9092
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT
log.dirs=$($kafkaData -replace '\\','/')
num.partitions=3
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
auto.create.topics.enable=false
"@ | Out-File -FilePath $serverProps -Encoding ascii
    }
    $metaProps = Join-Path $kafkaData "meta.properties"
    if (-not (Test-Path $metaProps)) {
        Write-Host "kafka: formatting KRaft storage (first run)"
        New-Item -ItemType Directory -Force $kafkaData | Out-Null
        # kafka-storage.bat via java directly (same 32KB cmd-line wall as the broker)
        $storageCp = Join-Path $KAFKA_HOME "libs\*"
        $clusterUuid = & (Join-Path $JAVA_HOME "bin\java.exe") -cp "$storageCp" `
            org.apache.kafka.tools.storage.Format `
            --cluster-id (New-Guid).ToString().Replace("-","").Substring(0,22) `
            --config $serverProps --ignore-formatted 2>&1
        if ($LASTEXITCODE -ne 0) { throw "kafka-storage format failed: $clusterUuid" }
    }
    Write-Host "kafka: starting"
    Start-Process -FilePath (Join-Path $JAVA_HOME "bin\java.exe") `
        -ArgumentList "-Xmx1G", "-Xms1G", `
            "-Dlog4j2.configurationFile=/$((Join-Path $KAFKA_HOME 'config\log4j2.yaml') -replace '\\','/')", `
            "-Dkafka.logs.dir=$((Join-Path $log 'kafka') -replace '\\','/')", `
            "-cp", "`"$(Join-Path $KAFKA_HOME 'libs\*')`"", `
            "kafka.Kafka", "`"$serverProps`"" `
        -RedirectStandardOutput (Join-Path $log "kafka.out.log") `
        -RedirectStandardError (Join-Path $log "kafka.err.log") `
        -WindowStyle Hidden
    for ($i = 0; $i -lt 45; $i++) {
        Start-Sleep -Seconds 2
        if (Test-Port 9092) { break }
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
    $srCp = Join-Path $SR_HOME "share\java\schema-registry\*"
    $srLog4j = ((Join-Path $SR_HOME "etc\schema-registry\log4j.properties") -replace '\\','/')
    Start-Process -FilePath (Join-Path $JAVA_HOME "bin\java.exe") `
        -ArgumentList "-cp", "`"$srCp`"", `
            "-Dlog4j.configuration=file:///$srLog4j", `
            "io.confluent.kafka.schemaregistry.rest.SchemaRegistryMain", "`"$srProps`"" `
        -RedirectStandardOutput (Join-Path $log "schema-registry.out.log") `
        -RedirectStandardError (Join-Path $log "schema-registry.err.log") `
        -WindowStyle Hidden
    for ($i = 0; $i -lt 45; $i++) {
        Start-Sleep -Seconds 2
        if (Test-Port 8081) { break }
    }
    if (-not (Test-Port 8081)) { throw "schema-registry did not come up on 8081; see $log\schema-registry.err.log" }
    Write-Host "schema-registry: up"
}

# ---- 3. Flink standalone cluster (2.x: no .bat scripts, direct JVM launch) ----
if (Test-Port 8082) {
    Write-Host "flink: already running (8082 open)"
} else {
    Write-Host "flink: starting local cluster (UI on 8082)"
    $flinkConf = Join-Path $FLINK_HOME "conf"
    $flinkDistJar = (Get-ChildItem (Join-Path $FLINK_HOME "lib") -Filter "flink-dist-*.jar" | Select-Object -First 1).FullName
    $flinkLibAll = Join-Path $FLINK_HOME "lib\*"
    $flinkCp = "$flinkDistJar;$flinkLibAll"
    $flinkLog4j = ((Join-Path $flinkConf "log4j.properties") -replace '\\','/')

    # JobManager (application-mode style standalone session)
    Start-Process -FilePath (Join-Path $JAVA_HOME "bin\java.exe") `
        -ArgumentList "-Xmx1024m", `
            "-Dlog.file=$(Join-Path $log 'flink-jobmanager.log')", `
            "-Dlog4j.configuration=file:///$flinkLog4j", `
            "-cp", "`"$flinkCp`"", `
            "org.apache.flink.runtime.entrypoint.ClusterEntrypoint", `
            "--configDir", "`"$flinkConf`"" `
        -WorkingDirectory $FLINK_HOME `
        -RedirectStandardOutput (Join-Path $log "flink-jm.out.log") `
        -RedirectStandardError (Join-Path $log "flink-jm.err.log") `
        -WindowStyle Hidden

    # TaskManager
    Start-Process -FilePath (Join-Path $JAVA_HOME "bin\java.exe") `
        -ArgumentList "-Xmx1024m", `
            "-Dlog.file=$(Join-Path $log 'flink-taskmanager.log')", `
            "-Dlog4j.configuration=file:///$flinkLog4j", `
            "-cp", "`"$flinkCp`"", `
            "org.apache.flink.runtime.taskexecutor.TaskManagerRunner", `
            "--configDir", "`"$flinkConf`"" `
        -WorkingDirectory $FLINK_HOME `
        -RedirectStandardOutput (Join-Path $log "flink-tm.out.log") `
        -RedirectStandardError (Join-Path $log "flink-tm.err.log") `
        -WindowStyle Hidden

    for ($i = 0; $i -lt 45; $i++) {
        Start-Sleep -Seconds 2
        if (Test-Port 8082) { break }
    }
    if (-not (Test-Port 8082)) { throw "flink UI did not come up on 8082; see $log" }
    Write-Host "flink: up (UI: http://localhost:8082)"
}

Write-Host ""
Write-Host "stack is up. next: python producer\setup_topics.py"
