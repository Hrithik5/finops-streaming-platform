"""
Silver layer writer.

Responsibility:
- Write validated Silver DataFrames to Delta tables.
- Generate table names from Kafka topics.

No parsing.
No transformations.
No data quality logic.
"""


from pyspark.sql import DataFrame


def topic_to_table_name(topic: str) -> str:
    """
    Convert a Kafka topic name into a Silver table name.

    Example:
        payment-events -> payment_events
    """

    return topic.replace("-", "_")


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
        .saveAsTable(table_name)
    )