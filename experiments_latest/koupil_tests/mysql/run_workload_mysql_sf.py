#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import argparse
from datetime import datetime
import pandas as pd
import pymysql

from workload_config import (
    TASK_DEFINITIONS,
    TASK_RUNS,
    DEFAULT_RUNS_PER_TASK,
    resolve_database_name
)

# ============================================================
# Argumentos
# ============================================================

def parse_args():
    parser = argparse.ArgumentParser(description="Run MySQL workload for a given SF")
    parser.add_argument("--sf", type=int, required=True, help="Scale Factor (ex: 1, 10, 30, 100)")
    return parser.parse_args()


# ============================================================
# Logging
# ============================================================

def log_title(msg):
    print("\n" + "=" * 70)
    print(msg)
    print("=" * 70)


def log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")


# ============================================================
# Execução (1 run)
# ============================================================

def run_query_once(db_config, task_name, sql, run_number):
    conn = pymysql.connect(**db_config)
    try:
        with conn.cursor() as cur:
            start = time.perf_counter()
            cur.execute(sql)
            rows = cur.fetchall()
            elapsed_ms = (time.perf_counter() - start) * 1000

            df = pd.DataFrame(rows, columns=[d[0] for d in cur.description])
            log(f"Task {task_name} | run {run_number} | {len(rows)} rows | {elapsed_ms:.2f} ms")
            return df, len(rows), elapsed_ms
    finally:
        conn.close()


# ============================================================
# Main
# ============================================================

def main():
    args = parse_args()
    sf = args.sf

    dbname = resolve_database_name(sf)

    DB_CONFIG = {
        "host": "127.0.0.1",
        "port": 3307,
        "user": "root",
        "password": "root",
        "database": dbname,
        "cursorclass": pymysql.cursors.Cursor,
    }

    # Padronizado igual Mongo: outputs/sf<SF>/
    output_dir = os.path.join("outputs", f"sf{sf}")
    os.makedirs(output_dir, exist_ok=True)

    log_title(f"MySQL Workload – SF{sf}")
    log(f"Database: {dbname}")
    log(f"Output directory: {output_dir}")

    summary_rows = []

    for task_name, sql in TASK_DEFINITIONS:
        runs = TASK_RUNS.get(task_name, DEFAULT_RUNS_PER_TASK)

        log_title(f"Running task: {task_name}")
        log(f"Configured runs: {runs}")

        run_times = []
        run_rows = []
        last_df = None

        for run in range(1, runs + 1):
            try:
                df, rows, elapsed_ms = run_query_once(DB_CONFIG, task_name, sql, run)
                run_times.append(elapsed_ms)
                run_rows.append(rows)
                last_df = df
            except Exception as e:
                log(f"ERROR running {task_name} (run {run}): {e}")

        if not run_times:
            continue

        avg_time = sum(run_times) / len(run_times)
        min_time = min(run_times)
        max_time = max(run_times)
        std_time = pd.Series(run_times).std(ddof=0) if len(run_times) > 1 else 0.0

        summary_rows.append({
            "task": task_name,
            "sf": sf,
            "engine": "mysql",
            "database": dbname,
            "collection_or_table": "N/A",
            "runs_configured": runs,
            "runs_valid": len(run_times),
            "result_rows": run_rows[-1],
            "avg_time_ms": round(avg_time, 2),
            "min_time_ms": round(min_time, 2),
            "max_time_ms": round(max_time, 2),
            "std_time_ms": round(std_time, 2),
        })

        # Resultado da última execução
        if last_df is not None and not last_df.empty:
            result_csv = os.path.join(output_dir, f"{task_name}_result.csv")
            last_df.to_csv(result_csv, index=False)

        # Tempos por run
        runs_df = pd.DataFrame({
            "run": list(range(1, len(run_times) + 1)),
            "time_ms": run_times,
            "rows": run_rows,
        })

        runs_csv = os.path.join(output_dir, f"{task_name}_runs.csv")
        runs_df.to_csv(runs_csv, index=False)

    # ========================================================
    # Resumo geral (mesmo nome do Mongo)
    # ========================================================

    summary_df = pd.DataFrame(summary_rows)
    summary_csv = os.path.join(output_dir, "workload_summary.csv")
    summary_df.to_csv(summary_csv, index=False)

    log_title("MySQL workload finished")
    log(f"Summary saved to: {summary_csv}")


if __name__ == "__main__":
    main()
