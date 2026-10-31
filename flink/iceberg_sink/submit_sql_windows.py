"""Run a Flink SQL script as a long-lived local streaming job, on Windows.

Flink's SQL client (sql-client.sh) is Linux-only upstream — no .bat ships with
any version. On native Windows the supported path into the same engine is
PyFlink's TableEnvironment: this script executes the SQL statements in-process
as a STREAMING TableEnvironment job.

Execution model (important, D11):
  The statements run in a LOCAL executor embedded in this Python process —
  they do NOT submit to a standalone Flink cluster on :8082. The local
  executor reads/writes the same Kafka topics and Iceberg files, and the SQL
  semantics (event-time, watermarks, exactly-once commits) are identical;
  only the dashboard differs. Run this in its own terminal and leave it up —
  it IS the job. Ctrl+C cancels it.

Usage:
    python flink/iceberg_sink/submit_sql_windows.py flink/sql/realtime_metrics.sql
"""

from __future__ import annotations

import argparse
import pathlib
import time

from pyflink.table import EnvironmentSettings, TableEnvironment


def load_statements(path: pathlib.Path) -> list[str]:
    text = path.read_text(encoding="utf-8")
    lines = [ln for ln in text.splitlines() if not ln.strip().startswith("--")]
    stmts = [s.strip() for s in "\n".join(lines).split(";") if s.strip()]
    return stmts


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("sql_file", type=pathlib.Path)
    ap.add_argument("--checkpoint-interval-ms", type=int, default=60_000,
                    help="exactly-once checkpoint interval for INSERT jobs (D5)")
    ap.add_argument("--no-checkpoint", action="store_true",
                    help="disable checkpointing (use for pure Kafka-sink jobs: with "
                         "checkpointing on, the KafkaSink defaults to EXACTLY_ONCE and "
                         "only flushes on completed checkpoints - which stalls output "
                         "on the local executor; observed empirically, see D11)")
    args = ap.parse_args()

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = TableEnvironment.create(settings)
    if not args.no_checkpoint:
        # exactly-once spine (D5) - required for the Iceberg sink
        t_env.get_config().set("execution.checkpointing.interval", f"{args.checkpoint_interval_ms}ms")
        t_env.get_config().set("execution.checkpointing.min-pause", "10s")
    t_env.get_config().set("parallelism.default", "1")

    statements = load_statements(args.sql_file)
    print(f"{args.sql_file}: {len(statements)} statements")
    print("NOTE: runs as a local streaming job in THIS process (D11). Leave it running.")

    inserts: list = []
    for i, stmt in enumerate(statements, 1):
        head = " ".join(stmt.upper().split())[:60]
        t0 = time.time()
        result = t_env.execute_sql(stmt)
        if head.startswith("INSERT"):
            inserts.append(result)
            print(f"  [{i}] INSERT accepted: {head}…")
        else:
            print(f"  [{i}] ok ({time.time() - t0:.1f}s): {head}…")

    if inserts:
        print(f"{len(inserts)} insert job(s) running in this process. Ctrl+C to stop.")
        # Block WITHOUT TableResult.wait(): on the local executor, wait() against an
        # unbounded stream tears down the py4j gateway JVM once the submit future
        # resolves (observed as ConnectionResetError 10054 right after 'INSERT
        # accepted'). The job itself is healthy — the wait is the killer. Just hold
        # the process open; Ctrl+C cancels the in-process mini-cluster.
        try:
            while True:
                time.sleep(5)
        except KeyboardInterrupt:
            print("stopped by user")


if __name__ == "__main__":
    main()
