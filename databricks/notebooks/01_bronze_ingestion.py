"""
01_bronze_ingestion.py

Kafka → Bronze orchestration notebook.

Flow:
    Kafka
      ↓
    read_kafka()
      ↓
    prepare_bronze()
      ↓
    write_bronze()
"""

from streaming.bronze.ingestion import (
    prepare_bronze,
    read_kafka,
    write_bronze,
)
from streaming.config import get_kafka_options
import inspect
from streaming.bronze import ingestion

# ---------------------------------------------------------------------
# Kafka Topics
# ---------------------------------------------------------------------

KAFKA_TOPICS = [
    "chargeback-events",
    "customer-events",
    "invoice-events",
    "merchant-events",
    "payment-events",
    "refund-events",
    "settlement-events",
]


# ---------------------------------------------------------------------
# Kafka Configuration
# ---------------------------------------------------------------------
# Credentials are retrieved from Databricks Secrets at runtime.

KAFKA_OPTIONS = get_kafka_options(dbutils)


# ---------------------------------------------------------------------
# Read Kafka
# ---------------------------------------------------------------------

kafka_df = read_kafka(
    spark,
    KAFKA_OPTIONS,
    KAFKA_TOPICS,
)

print("kafka_df.isStreaming =", kafka_df.isStreaming)
print(inspect.getsource(ingestion.read_kafka))


# ---------------------------------------------------------------------
# Prepare Bronze
# ---------------------------------------------------------------------

bronze_df = prepare_bronze(
    kafka_df,
)

print("bronze_df.isStreaming =", bronze_df.isStreaming)
# ---------------------------------------------------------------------
# Write Bronze
# ---------------------------------------------------------------------

write_bronze(
    bronze_df,
    "dev.bronze.raw_events",
    "/Volumes/dev/stream/streaming_checkpoints/bronze_raw_events_v2",
)


print(
    "Bronze ingestion completed successfully."
)