"""
Static seed ingestion.

Reads reference/transactional seed CSVs and writes
them to Bronze Delta tables.

No transformations.
"""

from pyspark.sql import DataFrame, SparkSession


def read_static_csv(
    spark: SparkSession,
    path: str,
) -> DataFrame:
    """Read a static CSV dataset."""
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(path)
    )


def write_static_bronze(
    df: DataFrame,
    table_name: str,
) -> None:
    """Write a static dataset to a Bronze Delta table."""
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(table_name)
    )