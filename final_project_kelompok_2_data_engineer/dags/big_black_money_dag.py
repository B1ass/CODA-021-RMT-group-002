import os
import sys
import subprocess

from datetime import datetime, timedelta

import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator



PROJECT_PATH = "/opt/airflow/project"
MAIN_PATH = os.path.join(
    PROJECT_PATH,
    "scripts",
    "main.py"
)


# TASK ETL

def run_etl():

    print("=== BIG BLACK MONEY ETL START ===")

    if not os.path.exists(MAIN_PATH):
        raise FileNotFoundError(
            f"main.py tidak ditemukan: {MAIN_PATH}"
        )

    print(f"Menjalankan: {MAIN_PATH}")

    result = subprocess.run(
        [
            sys.executable,
            MAIN_PATH
        ],
        cwd=PROJECT_PATH,
        check=True
    )

    print(
        f"ETL selesai dengan return code: "
        f"{result.returncode}"
    )

    print("=== BIG BLACK MONEY ETL SELESAI ===")


# DAG

# Automatisasi: pipeline dijalankan otomatis 1x sehari
# (schedule sebelumnya None / manual trigger only).
# Jadwal jam 01:00 WIB dipilih sebagai default yang aman

default_args = {
    "owner": "data-engineer",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="big_black_money_etl",
    default_args=default_args,
    start_date=pendulum.datetime(
        2026,
        9,
        21,
        tz="Asia/Jakarta"
    ),
    schedule="0 1 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["Big Black Money", "ETL", "PostgreSQL"],
) as dag:

    etl = PythonOperator(
        task_id="run_big_black_money_etl",
        python_callable=run_etl,
    )
