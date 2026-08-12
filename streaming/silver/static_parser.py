"""
Static Silver parser.

Responsibility:
- Read static Bronze tables.
- Return them as structured DataFrames.

No Kafka logic.
No business transformations.
"""

from pyspark.sql import DataFrame, SparkSession


def read_static_bronze(
    spark: SparkSession,
    table_name: str,
) -> DataFrame:
    """
    Read a static Bronze Delta table.
    """

    return spark.read.table(table_name)