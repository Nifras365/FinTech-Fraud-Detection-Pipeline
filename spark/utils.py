import logging
import os
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession


logger = logging.getLogger("spark-utils")


@dataclass(frozen=True)
class SparkAppConfig:
    app_name: str = os.getenv("SPARK_APP_NAME", "fraud-detection")
    kafka_bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    kafka_topic: str = os.getenv("KAFKA_TOPIC", "transactions")
    high_value_threshold: float = float(os.getenv("HIGH_VALUE_THRESHOLD", "5000"))
    impossible_travel_window_minutes: int = int(os.getenv("IMPOSSIBLE_TRAVEL_WINDOW_MINUTES", "10"))
    watermark_delay: str = os.getenv("WATERMARK_DELAY", "15 minutes")
    postgres_host: str = os.getenv("POSTGRES_HOST", "postgres")
    postgres_port: int = int(os.getenv("POSTGRES_PORT", "5432"))
    postgres_db: str = os.getenv("POSTGRES_DB", "frauddb")
    postgres_user: str = os.getenv("POSTGRES_USER", "fraud_user")
    postgres_password: str = os.getenv("POSTGRES_PASSWORD", "fraud_pass")
    checkpoint_root: str = os.getenv("SPARK_CHECKPOINT_ROOT", "/tmp/spark-checkpoints")

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"

    @property
    def jdbc_properties(self) -> dict[str, str]:
        return {
            "user": self.postgres_user,
            "password": self.postgres_password,
            "driver": "org.postgresql.Driver",
        }


def get_config() -> SparkAppConfig:
    return SparkAppConfig()


def create_spark_session(config: SparkAppConfig) -> SparkSession:
    spark = (
        SparkSession.builder.appName(config.app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def write_batch_to_postgres(batch_df: DataFrame, table_name: str, config: SparkAppConfig) -> None:
    if batch_df.isEmpty():
        logger.info("No records to write for table=%s", table_name)
        return

    logger.info("Writing microbatch rows=%s to table=%s", batch_df.count(), table_name)
    (
        batch_df.write.mode("append")
        .jdbc(url=config.jdbc_url, table=table_name, properties=config.jdbc_properties)
    )
