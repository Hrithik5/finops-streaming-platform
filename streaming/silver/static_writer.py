"""
Static Silver writer.

Responsibility:
- Write validated static DataFrames to Silver Delta tables.
"""

from pyspark.sql import DataFrame


def write_static_silver(
    df: DataFrame,
    table_name: str,
) -> None:
    """
    Write a static DataFrame to a Silver Delta table.
    """
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )