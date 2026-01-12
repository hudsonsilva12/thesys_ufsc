#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import os
import time
from datetime import datetime
import statistics as stats
import pymysql
import pandas as pd

# ==========================
# LER PARÂMETRO --sf
# ==========================
parser = argparse.ArgumentParser()
parser.add_argument("--sf", type=int, default=1, help="Scale factor (define o banco ecommerce_sf<SF>)")
args = parser.parse_args()

from workload_config import resolve_database_name
DB_NAME = resolve_database_name(args.sf)


print(f"\n📌 Workload apontando para o banco: {DB_NAME}\n")

# ==========================
# CONFIG DO BANCO
# ==========================
DB_CONFIG = {
    "host": "127.0.0.1",
    "port": 3308,
    "user": "root",
    "password": "root",
    "database": DB_NAME,
    "cursorclass": pymysql.cursors.Cursor,
}

OUTPUT_DIR = f"outputs_sf{args.sf}"

# ==========================
# IMPORTAR CONFIG DO WORKLOAD
# ==========================
from workload_config import TASK_DEFINITIONS, TASK_RUNS, DEFAULT_RUNS_PER_TASK

RESET = "\033[0m"
BOLD = "\033[1m"
CYAN = "\033[36m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
MAGENTA = "\033[35m"
RED = "\033[31m"


def log(msg, color=""):
    now = datetime.now().strftime("%H:%M:%S")
    print(f"{color}[{now}] {msg}{RESET}")


def ensure_output_dir():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR, exist_ok=True)


def run_query_once(task_name, sql, run_number):
    ensure_output_dir()
    log(f"=== {task_name} (run {run_number}) ===", CYAN)

    conn = pymysql.connect(**DB_CONFIG)

    try:
        with conn.cursor() as cur:
            t0 = time.perf_counter()
            cur.execute(sql)
            rows = cur.fetchall()
            elapsed_ms = (time.perf_counter() - t0) * 1000
            df = pd.DataFrame(rows, columns=[d[0] for d in cur.description])
            log(f"OK: {len(rows)} linhas ({elapsed_ms:.2f} ms)", GREEN)
            return df, len(rows), elapsed_ms
    finally:
        conn.close()


def main():
    ensure_output_dir()
    log(f"Iniciando workload para banco {DB_NAME}", BOLD + CYAN)

    metrics = {}

    for task_name, sql in TASK_DEFINITIONS:
        runs = TASK_RUNS.get(task_name, DEFAULT_RUNS_PER_TASK)
        log(f"Task {task_name}: {runs} runs", MAGENTA)

        metrics[task_name] = []
        last_df = None

        for r in range(1, runs + 1):
            df, rows, ms = run_query_once(task_name, sql, r)
            metrics[task_name].append({"run": r, "rows": rows, "elapsed_ms": ms})
            last_df = df

        if last_df is not None:
            last_df.to_csv(f"{OUTPUT_DIR}/{task_name}.csv", index=False)

        pd.DataFrame(metrics[task_name]).to_csv(
            f"{OUTPUT_DIR}/{task_name}_runs.csv", index=False
        )

    # resumo geral
    summary = []
    for task_name, values in metrics.items():
        times = [v["elapsed_ms"] for v in values]
        avg = stats.mean(times)
        summary.append({"task": task_name, "avg_ms": avg})

    pd.DataFrame(summary).to_csv(f"{OUTPUT_DIR}/summary.csv", index=False)
    log("Workload concluído!", GREEN)


if __name__ == "__main__":
    main()
