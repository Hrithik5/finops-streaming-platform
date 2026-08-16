"""
Silver layer writer.

Responsibility:
- Incrementally write validated Silver microbatches.
- Deduplicate using business keys.
- Prevent replayed Kafka records from creating duplicate business records.

No parsing.
No transformations.
No data quality logic.
"""

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

# ---------------------------------------------------------------------
# Topic → Silver Table
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
# Topic → Business Key
# ---------------------------------------------------------------------

TOPIC_KEY_MAPPING = {
    "merchant-events": ["merchant_id"],
    "customer-events": ["customer_id"],
    "invoice-events": ["invoice_id"],
    "payment-events": ["payment_id"],
    "refund-events": ["refund_id"],
    "chargeback-events": ["chargeback_id"],
    "settlement-events": ["settlement_id"],
}


# ---------------------------------------------------------------------
# Table Name
# ---------------------------------------------------------------------


def topic_to_table_name(
    topic: str,
) -> str:
    """Return the Silver table name for a Kafka topic."""

    if topic not in TOPIC_TABLE_MAPPING:
        raise ValueError(f"No Silver table mapping found for topic: {topic}")

    return TOPIC_TABLE_MAPPING[topic]


# ---------------------------------------------------------------------
# Business Key
# ---------------------------------------------------------------------


def topic_to_key_columns(
    topic: str,
) -> list[str]:
    """Return the business key columns for a Kafka topic."""

    if topic not in TOPIC_KEY_MAPPING:
        raise ValueError(f"No business key mapping found for topic: {topic}")

    return TOPIC_KEY_MAPPING[topic]


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
    Incrementally merge a validated microbatch into Silver.

    Business keys define record identity.
    Duplicate keys inside the same microbatch are reduced
    to the latest Kafka record before MERGE.
    """

    table_name = f"{catalog}.{schema}.{topic_to_table_name(topic)}"

    key_columns = topic_to_key_columns(topic)

    # -------------------------------------------------------------
    # Deduplicate incoming microbatch
    # -------------------------------------------------------------

    window = Window.partitionBy(*key_columns).orderBy(
        F.col("kafka_timestamp").desc_nulls_last(),
        F.col("partition").desc_nulls_last(),
        F.col("offset").desc_nulls_last(),
    )

    deduplicated_df = (
        df.withColumn(
            "_row_number",
            F.row_number().over(window),
        )
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )

    target = DeltaTable.forName(
        df.sparkSession,
        table_name,
    )

    merge_condition = " AND ".join(
        f"target.{column} = source.{column}" for column in key_columns
    )

    (
        target.alias("target")
        .merge(
            deduplicated_df.alias("source"),
            merge_condition,
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
