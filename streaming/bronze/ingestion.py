"""
Bronze ingestion utilities.

Responsibility:
- Read events from Kafka.
- Write raw Kafka events to a Delta Bronze table.

No business transformations.
No topic-specific schemas.
"""

from pyspark.sql import DataFrame, SparkSession


def read_kafka(
    spark: SparkSession,
    kafka_options: dict[str, str],
    topics: list[str],
    starting_offsets: str = "earliest",
    ending_offsets: str = "latest",
) -> DataFrame:
    """
    Read events from Kafka into a Spark DataFrame.

    This function performs no transformations.
    """

    return (
        spark.read.format("kafka")
        .options(**kafka_options)
        .option("subscribe", ",".join(topics))
        .option("startingOffsets", starting_offsets)
        .option("endingOffsets", ending_offsets)
        .load()
    )


def prepare_bronze(df: DataFrame) -> DataFrame:
    """
    Convert raw Kafka records into the Bronze structure.

    Bronze columns:
        topic
        partition
        offset
        kafka_timestamp
        raw_payload
    """

    return df.select(
        "topic",
        "partition",
        "offset",
        df.timestamp.alias("kafka_timestamp"),
        df.value.cast("string").alias("raw_payload"),
    )


def write_bronze(
    df: DataFrame,
    table_name: str,
    checkpoint_location: str,
) -> None:
    """
    Write Bronze data as a Delta table using Structured Streaming.
    """

    (
        df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_location)
        .trigger(availableNow=True)
        .toTable(table_name)
    )
