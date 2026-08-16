"""
01_bronze_ingestion.py

Bronze ingestion pipeline.

Reads events from Kafka and prepares them
for the Bronze Delta layer.
"""

from streaming.bronze.ingestion import (
    prepare_bronze,
    read_kafka,
    write_bronze,
)
from streaming.config import get_kafka_options


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


# ---------------------------------------------------------------------
# Prepare Bronze
# ---------------------------------------------------------------------

bronze_df = prepare_bronze(
    kafka_df,
)


# ---------------------------------------------------------------------
# Write Bronze
# ---------------------------------------------------------------------

write_bronze(
    bronze_df,
    "dev.bronze.raw_events",
    "/Volumes/dev/stream/streaming_checkpoints/bronze",
)
