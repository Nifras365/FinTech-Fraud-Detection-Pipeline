from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


transaction_schema = StructType(
    [
        StructField("user_id", StringType(), False),
        StructField("timestamp", StringType(), False),
        StructField("merchant_category", StringType(), False),
        StructField("amount", DoubleType(), False),
        StructField("location", StringType(), False),
    ]
)


state_schema = StructType(
    [
        StructField("last_event_ts", TimestampType(), True),
        StructField("last_location", StringType(), True),
    ]
)


classified_output_schema = StructType(
    [
        StructField("user_id", StringType(), False),
        StructField("timestamp", StringType(), False),
        StructField("event_ts", TimestampType(), False),
        StructField("merchant_category", StringType(), False),
        StructField("amount", DoubleType(), False),
        StructField("location", StringType(), False),
        StructField("fraud_reason", StringType(), True),
        StructField("is_fraud", BooleanType(), False),
    ]
)
