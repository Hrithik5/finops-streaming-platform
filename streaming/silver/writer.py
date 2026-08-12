"""
Silver layer writer.

Responsibility:
- Write validated Silver DataFrames to Delta tables.
- Generate Silver table names from Kafka topics.

No parsing.
No transformations.
No data quality logic.
"""

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
# Write Silver
# ---------------------------------------------------------------------


def write_silver(
    df: DataFrame,
    topic: str,
    catalog: str = "dev",
    schema: str = "silver",
) -> None:
    """
    Write a validated DataFrame to its Silver Delta table.
    """

    table_name = (
        f"{catalog}."
        f"{schema}."
        f"{topic_to_table_name(topic)}"
    )

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )