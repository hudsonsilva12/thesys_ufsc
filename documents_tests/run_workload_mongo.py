import os
import time
import argparse
from datetime import datetime

import pandas as pd
from pymongo import MongoClient

from workload_config_mongo import (
    TASK_DEFINITIONS,
    TASK_RUNS,
    DEFAULT_RUNS_PER_TASK,
    MONGO_URI,
    MONGO_DB_BY_SF,
)

# ============================================================
# Argumentos de linha de comando
# ============================================================

def parse_args():
    parser = argparse.ArgumentParser(
        description="Run MongoDB workload for a given Scale Factor (SF)"
    )
    parser.add_argument(
        "--sf",
        type=int,
        required=True,
        choices=[1, 10, 30, 100],
        help="Scale Factor (1, 10, 30, 100)",
    )
    return parser.parse_args()


# ============================================================
# Conexão MongoDB
# ============================================================

def connect_mongo(sf: int):
    if sf not in MONGO_DB_BY_SF:
        raise ValueError(f"SF {sf} not configured in MONGO_DB_BY_SF")

    dbname = MONGO_DB_BY_SF[sf]
    client = MongoClient(MONGO_URI)
    db = client[dbname]
    return client, db


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
# Execução de uma pipeline (1 run)
# ============================================================

def run_pipeline_once(task_name, collection_name, pipeline, run_number, sf):
    client, db = connect_mongo(sf)

    start = time.perf_counter()
    cursor = db[collection_name].aggregate(
        pipeline,
        allowDiskUse=True
    )
    rows = list(cursor)
    elapsed_ms = (time.perf_counter() - start) * 1000

    client.close()

    df = pd.DataFrame(rows)

    log(
        f"Task {task_name} | run {run_number} | "
        f"{len(rows)} rows | {elapsed_ms:.2f} ms"
    )

    return df, len(rows), elapsed_ms


# ============================================================
# Main
# ============================================================

def main():
    args = parse_args()
    sf = args.sf

    dbname = MONGO_DB_BY_SF[sf]

    # Diretório de saída isolado por SF
    output_dir = os.path.join("outputs", f"sf{sf}")
    os.makedirs(output_dir, exist_ok=True)

    log_title(f"MongoDB Workload – SF{sf}")
    log(f"Database: {dbname}")
    log(f"Output directory: {output_dir}")

    summary_rows = []

    for task_name, task in TASK_DEFINITIONS.items():
        collection = task["collection"]
        pipeline = task["pipeline"]

        runs = TASK_RUNS.get(task_name, DEFAULT_RUNS_PER_TASK)

        log_title(f"Running task: {task_name}")
        log(f"Collection: {collection}")
        log(f"Configured runs: {runs}")

        run_times = []
        run_rows = []
        last_df = None

        for run in range(1, runs + 1):
            try:
                df, rows, elapsed_ms = run_pipeline_once(
                    task_name,
                    collection,
                    pipeline,
                    run,
                    sf
                )
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
        std_time = (
            pd.Series(run_times).std(ddof=0)
            if len(run_times) > 1
            else 0.0
        )

        summary_rows.append({
            "task": task_name,
            "sf": sf,
            "database": dbname,
            "collection": collection,
            "runs_configured": runs,
            "runs_valid": len(run_times),
            "example_rows": run_rows[-1],
            "avg_time_ms": round(avg_time, 2),
            "min_time_ms": round(min_time, 2),
            "max_time_ms": round(max_time, 2),
            "std_time_ms": round(std_time, 2),
        })

        # Resultado da última execução
        if last_df is not None and not last_df.empty:
            result_csv = os.path.join(
                output_dir,
                f"{task_name}_result.csv"
            )
            last_df.to_csv(result_csv, index=False)

        # Tempos por run
        runs_df = pd.DataFrame({
            "run": list(range(1, len(run_times) + 1)),
            "time_ms": run_times,
            "rows": run_rows,
        })

        runs_csv = os.path.join(
            output_dir,
            f"{task_name}_runs.csv"
        )
        runs_df.to_csv(runs_csv, index=False)

    # ========================================================
    # Resumo geral
    # ========================================================

    summary_df = pd.DataFrame(summary_rows)
    summary_csv = os.path.join(
        output_dir,
        "workload_summary_mongo.csv"
    )
    summary_df.to_csv(summary_csv, index=False)

    log_title("MongoDB workload finished")
    log(f"Summary saved to: {summary_csv}")


if __name__ == "__main__":
    main()

