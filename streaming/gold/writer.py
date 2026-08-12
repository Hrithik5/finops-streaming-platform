"""
Gold layer writer.

Responsibility:
- Write Gold DataFrames to Delta tables.

No transformations.
No joins.
No business logic.
"""

from pyspark.sql import DataFrame


def write_gold(
    df: DataFrame,
    table_name: str,
) -> None:
    """Write a Gold DataFrame to a Delta table."""

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )