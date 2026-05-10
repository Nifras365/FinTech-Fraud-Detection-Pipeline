import logging
from datetime import timedelta
from typing import Iterator

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.streaming.state import GroupStateTimeout

from schema import classified_output_schema, state_schema, transaction_schema
from utils import create_spark_session, get_config, write_batch_to_postgres


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("fraud-detection")


def classify_user_stream(key: tuple[str], pdf_iter: Iterator[pd.DataFrame], state) -> Iterator[pd.DataFrame]:
    """
    Stateful user-level fraud detection.

    Event time vs processing time note:
    - Event time comes from the transaction payload timestamp (business occurrence time).
    - Processing time is when Spark receives/processes the record.
    - Impossible travel must use event time to avoid false negatives caused by delays/out-of-order ingestion.
    """
    user_id = key[0]

    last_event_ts = None
    last_location = None
    if state.exists:
        prior = state.get
        last_event_ts = prior[0]
        last_location = prior[1]

    rows: list[dict] = []

    for pdf in pdf_iter:
        if pdf.empty:
            continue

        sorted_pdf = pdf.sort_values("event_ts")

        for _, row in sorted_pdf.iterrows():
            event_ts = pd.Timestamp(row["event_ts"]).to_pydatetime()
            location = str(row["location"])
            amount = float(row["amount"])

            fraud_reason = None
            is_fraud = False

            if amount > 5000:
                is_fraud = True
                fraud_reason = "HIGH_VALUE"

            if last_event_ts is not None and last_location is not None:
                if location != last_location:
                    delta_seconds = (event_ts - last_event_ts).total_seconds()
                    if 0 <= delta_seconds <= 600:
                        is_fraud = True
                        fraud_reason = "IMPOSSIBLE_TRAVEL"

            rows.append(
                {
                    "user_id": user_id,
                    "timestamp": str(row["timestamp"]),
                    "event_ts": event_ts,
                    "merchant_category": str(row["merchant_category"]),
                    "amount": amount,
                    "location": location,
                    "fraud_reason": fraud_reason,
                    "is_fraud": is_fraud,
                }
            )

            last_event_ts = event_ts
            last_location = location

    if last_event_ts is not None:
        state.update((last_event_ts, last_location))
        timeout_ts_ms = int((last_event_ts + timedelta(minutes=20)).timestamp() * 1000)
        state.setTimeoutTimestamp(timeout_ts_ms)

    if rows:
        yield pd.DataFrame(rows)


def build_source_stream() -> DataFrame:
    cfg = get_config()
    spark = create_spark_session(cfg)

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", cfg.kafka_bootstrap_servers)
        .option("subscribe", cfg.kafka_topic)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed_df = (
        kafka_df.selectExpr("CAST(value AS STRING) AS payload")
        .select(from_json(col("payload"), transaction_schema).alias("tx"))
        .select("tx.*")
        .withColumn("event_ts", to_timestamp(col("timestamp")))
        .filter(col("event_ts").isNotNull())
        .withWatermark("event_ts", cfg.watermark_delay)
    )

    return parsed_df


def run_pipeline() -> None:
    cfg = get_config()
    parsed_df = build_source_stream()

    classified_df = parsed_df.groupBy("user_id").applyInPandasWithState(
        func=classify_user_stream,
        outputStructType=classified_output_schema,
        stateStructType=state_schema,
        outputMode="append",
        timeoutConf=GroupStateTimeout.EventTimeTimeout,
    )

    fraud_df = classified_df.filter(col("is_fraud") == True).select(
        "user_id",
        "timestamp",
        "amount",
        "merchant_category",
        "location",
        "fraud_reason",
    )

    validated_df = classified_df.filter(col("is_fraud") == False).select(
        "user_id",
        "timestamp",
        "amount",
        "merchant_category",
        "location",
    )

    fraud_query = (
        fraud_df.writeStream.outputMode("append")
        .foreachBatch(lambda df, _: write_batch_to_postgres(df, "fraud_alerts", cfg))
        .option("checkpointLocation", f"{cfg.checkpoint_root}/fraud_alerts")
        .start()
    )

    validated_query = (
        validated_df.writeStream.outputMode("append")
        .foreachBatch(lambda df, _: write_batch_to_postgres(df, "validated_transactions", cfg))
        .option("checkpointLocation", f"{cfg.checkpoint_root}/validated_transactions")
        .start()
    )

    logger.info("Spark streaming pipeline started.")

    fraud_query.awaitTermination()
    validated_query.awaitTermination()


if __name__ == "__main__":
    run_pipeline()
