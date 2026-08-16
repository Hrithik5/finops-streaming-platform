"""
Silver layer writer.

Responsibility:
- Write validated Silver microbatches to Delta tables.
- Maintain idempotent writes using Kafka topic/partition/offset.

No parsing.
No transformations.
No data quality logic.
"""

from delta.tables import DeltaTable
from pyspark.sql import DataFrame


# ---------------------------------------------------------------------
# Topic → Silver Table Mapping
# ---------------------------------------------------------------------

TOPIC_TABLE_MAPPING = {
    "merchant-events": "merchants",
    "customer-events": "customers",
    "invoice-events": "invoices",
    "payment-events": "payments",
    "refund-events": "refunds",
    "chargeback-events": "chargebacks",
    "settlement-events": "settlements",
}


# ---------------------------------------------------------------------
# Table Name
# ---------------------------------------------------------------------


def topic_to_table_name(topic: str) -> str:
    """
    Convert a Kafka topic into its Silver table name.
    """

    if topic not in TOPIC_TABLE_MAPPING:
        raise ValueError(
            f"No Silver table mapping found for topic: {topic}"
        )

    return TOPIC_TABLE_MAPPING[topic]


# ---------------------------------------------------------------------
# Incremental Silver Writer
# ---------------------------------------------------------------------


def write_silver(
    df: DataFrame,
    topic: str,
    catalog: str = "dev",
    schema: str = "silver",
) -> None:
    """
    Idempotently write a Silver microbatch to Delta.

    Kafka records are uniquely identified by:
        topic + partition + offset

    Existing records are not rewritten.
    """

    table_name = (
        f"{catalog}."
        f"{schema}."
        f"{topic_to_table_name(topic)}"
    )

    target = DeltaTable.forName(
        df.sparkSession,
        table_name,
    )

    (
        target.alias("target")
        .merge(
            df.alias("source"),
            """
            target.topic = source.topic
            AND target.partition = source.partition
            AND target.offset = source.offset
            """,
        )
        .whenNotMatchedInsertAll()
        .execute()
    )