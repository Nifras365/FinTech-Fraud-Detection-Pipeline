import csv
import json
import logging
import os
from datetime import datetime

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("fraud-reconciliation-dag")

REPORTS_DIR = os.getenv("REPORTS_DIR", "/opt/airflow/reports")
TMP_DIR = os.path.join(REPORTS_DIR, "_tmp")
VALIDATED_PARQUET = os.path.join(TMP_DIR, "validated_transactions.parquet")
TOTALS_JSON = os.path.join(TMP_DIR, "totals.json")
RECON_REPORT = os.path.join(REPORTS_DIR, "reconciliation_report.csv")
ANALYTICS_REPORT = os.path.join(REPORTS_DIR, "fraud_by_merchant_category.csv")
POSTGRES_CONN_ID = os.getenv("AIRFLOW_POSTGRES_CONN_ID", "postgres_default")


def _ensure_dirs() -> None:
    os.makedirs(REPORTS_DIR, exist_ok=True)
    os.makedirs(TMP_DIR, exist_ok=True)


def task_read_validated_transactions() -> str:
    _ensure_dirs()
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    sql = "SELECT user_id, timestamp, amount, merchant_category, location FROM validated_transactions"
    df = hook.get_pandas_df(sql)

    df.to_parquet(VALIDATED_PARQUET, index=False)
    logger.info("Validated transactions extracted: %s rows", len(df))
    return VALIDATED_PARQUET


def task_aggregate_totals() -> str:
    _ensure_dirs()
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    validated_df = pd.read_parquet(VALIDATED_PARQUET) if os.path.exists(VALIDATED_PARQUET) else pd.DataFrame()
    fraud_df = hook.get_pandas_df("SELECT amount FROM fraud_alerts")

    validated_amount = float(validated_df["amount"].sum()) if not validated_df.empty else 0.0
    fraud_amount = float(fraud_df["amount"].sum()) if not fraud_df.empty else 0.0
    total_ingress_amount = validated_amount + fraud_amount

    totals = {
        "generated_at_utc": datetime.utcnow().isoformat(),
        "total_ingress_amount": round(total_ingress_amount, 2),
        "validated_amount": round(validated_amount, 2),
        "fraud_amount": round(fraud_amount, 2),
    }

    with open(TOTALS_JSON, "w", encoding="utf-8") as fp:
        json.dump(totals, fp, indent=2)

    logger.info("Aggregation complete: %s", totals)
    return TOTALS_JSON


def task_generate_reconciliation_report() -> str:
    _ensure_dirs()
    with open(TOTALS_JSON, "r", encoding="utf-8") as fp:
        totals = json.load(fp)

    with open(RECON_REPORT, "w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(
            csv_file,
            fieldnames=[
                "generated_at_utc",
                "total_ingress_amount",
                "validated_amount",
                "fraud_amount",
            ],
        )
        writer.writeheader()
        writer.writerow(totals)

    logger.info("Reconciliation report generated: %s", RECON_REPORT)
    return RECON_REPORT


def task_generate_fraud_analytics() -> str:
    _ensure_dirs()
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    analytics_sql = """
        SELECT
            merchant_category,
            COUNT(*) AS fraud_attempts
        FROM fraud_alerts
        GROUP BY merchant_category
        ORDER BY fraud_attempts DESC
    """
    analytics_df = hook.get_pandas_df(analytics_sql)

    analytics_df.to_csv(ANALYTICS_REPORT, index=False)
    logger.info("Fraud analytics report generated: %s", ANALYTICS_REPORT)
    return ANALYTICS_REPORT


with DAG(
    dag_id="fraud_reconciliation_dag",
    schedule="0 */6 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["fraud", "reconciliation", "fintech"],
) as dag:
    read_validated_transactions = PythonOperator(
        task_id="read_validated_transactions",
        python_callable=task_read_validated_transactions,
    )

    aggregate_totals = PythonOperator(
        task_id="aggregate_totals",
        python_callable=task_aggregate_totals,
    )

    generate_reconciliation_report = PythonOperator(
        task_id="generate_reconciliation_report",
        python_callable=task_generate_reconciliation_report,
    )

    generate_fraud_analytics = PythonOperator(
        task_id="generate_fraud_analytics",
        python_callable=task_generate_fraud_analytics,
    )

    read_validated_transactions >> aggregate_totals >> generate_reconciliation_report
    aggregate_totals >> generate_fraud_analytics
