"""
Bronze ingestion.

Responsibility:
- Read events from Kafka using Structured Streaming.
- Prepare raw Kafka records.
- Write raw events to the Bronze Delta table.

No business transformations.
No topic-specific schemas.
"""

from pyspark.sql import DataFrame, SparkSession


# ---------------------------------------------------------------------
# Kafka Reader
# ---------------------------------------------------------------------


def read_kafka(
    spark: SparkSession,
    kafka_options: dict[str, str],
    topics: list[str],
    starting_offsets: str = "earliest",
) -> DataFrame:
    """
    Read Kafka events as a Structured Streaming DataFrame.
    """

    return (
        spark.readStream
        .format("kafka")
        .options(**kafka_options)
        .option("subscribe", ",".join(topics))
        .option("startingOffsets", starting_offsets)
        .load()
    )


# ---------------------------------------------------------------------
# Bronze Preparation
# ---------------------------------------------------------------------


def prepare_bronze(
    df: DataFrame,
) -> DataFrame:
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


# ---------------------------------------------------------------------
# Bronze Writer
# ---------------------------------------------------------------------


def write_bronze(
    df: DataFrame,
    table_name: str,
    checkpoint_location: str,
) -> None:
    """
    Write Kafka streaming data to a Delta Bronze table.

    AvailableNow processes all currently available Kafka data,
    commits the checkpoint, and then stops.
    """

    (
        df.writeStream
        .format("delta")
        .outputMode("append")
        .option(
            "checkpointLocation",
            checkpoint_location,
        )
        .trigger(availableNow=True)
        .toTable(table_name)
    )