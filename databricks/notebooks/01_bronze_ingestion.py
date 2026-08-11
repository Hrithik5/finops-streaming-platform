"""
01_bronze_ingestion.py

Bronze ingestion pipeline.

Reads events from Kafka and prepares them
for the Bronze Delta layer.
"""

from streaming.bronze.ingestion import (
    read_kafka,
    prepare_bronze,
    write_bronze,
)


TOPICS = [
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
# Loaded/provided at runtime.
# Do NOT hardcode credentials in this notebook.

kafka_options = {}


# ---------------------------------------------------------------------
# Read Kafka
# ---------------------------------------------------------------------

kafka_df = read_kafka(
    spark,
    kafka_options,
    TOPICS,
)


# ---------------------------------------------------------------------
# Prepare Bronze
# ---------------------------------------------------------------------

bronze_df = prepare_bronze(kafka_df)


# ---------------------------------------------------------------------
# Write Bronze
# ---------------------------------------------------------------------

write_bronze(
    bronze_df,
    "dev.bronze.raw_events",
    "/Volumes/dev/stream/streaming_checkpoints/bronze",
)