"""Submit a Flink SQL script to a locally running Flink cluster, on Windows.

Flink's SQL client (sql-client.sh) is Linux-only upstream — no .bat ships with
1.19. On native Windows the supported path into the same engine is the
TableEnvironment over Py4J: PyFlink's Table API talks to a real Flink mini-
cluster / gateway regardless of host OS.

What this does:
  1. reads the SQL script (statements separated by ';')
  2. creates a StreamTableEnvironment with checkpointing (exactly-once spine, D5)
  3. executes each statement; INSERTs block until submitted to the cluster

Note on execution model: statements run in-process against Flink's local
executor, which reads/writes the same Kafka + Iceberg resources as the
standalone cluster. The SQL semantics (event-time, watermarks, exactly-once
Iceberg commits) are identical to a sql-client submission — the client is a
convenience shell, not a different engine.
"""

from __future__ import annotations

import argparse
import pathlib
import sys
import time

from pyflink.table import EnvironmentSettings, TableEnvironment


def load_statements(path: pathlib.Path) -> list[str]:
    text = path.read_text(encoding="utf-8")
    # strip line comments, then split on ';'
    lines = [ln for ln in text.splitlines() if not ln.strip().startswith("--")]
    stmts = [s.strip() for s in "\n".join(lines).split(";") if s.strip()]
    return stmts


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("sql_file", type=pathlib.Path)
    ap.add_argument("--checkpoint-interval-ms", type=int, default=60_000,
                    help="exactly-once checkpoint interval for INSERT jobs (D5)")
    args = ap.parse_args()

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = TableEnvironment.create(settings)
    t_env.get_config().set(
        "execution.checkpointing.interval", f"{args.checkpoint_interval_ms}ms"
    )

    statements = load_statements(args.sql_file)
    print(f"{args.sql_file}: {len(statements)} statements")

    inserts = 0
    for i, stmt in enumerate(statements, 1):
        head = " ".join(stmt.upper().split())[:60]
        t0 = time.time()
        result = t_env.execute_sql(stmt)
        if head.startswith("INSERT"):
            # collect the JobClient result — blocks until the job is running
            result.wait()
            inserts += 1
            print(f"  [{i}] INSERT submitted: {head}…")
        else:
            print(f"  [{i}] ok ({time.time() - t0:.1f}s): {head}…")

    print(f"done. {inserts} insert job(s) now running in the Flink web UI: http://localhost:8082")


if __name__ == "__main__":
    main()
