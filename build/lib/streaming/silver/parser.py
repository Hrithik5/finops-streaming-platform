"""
Silver layer parser.

Responsibility:
- Parse raw JSON payloads using the correct event schema.
- Select the schema based on Kafka topic.
- Produce structured Silver-ready DataFrames.

No business transformations.
No data quality logic.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_json

from streaming.silver.schemas import EVENT_SCHEMAS


def parse_event(df: DataFrame, topic: str) -> DataFrame:
    """
    Parse raw event JSON for a specific Kafka topic.

    Parameters
    ----------
    df:
        Bronze DataFrame containing raw_payload.
    topic:
        Kafka topic whose schema should be applied.

    Returns
    -------
    DataFrame
        Structured DataFrame containing parsed event data.
    """

    if topic not in EVENT_SCHEMAS:
        raise ValueError(
            f"No schema registered for topic: {topic}"
        )

    schema = EVENT_SCHEMAS[topic]

    return (
        df
        .filter(col("topic") == topic)
        .withColumn(
            "event",
            from_json(
                col("raw_payload"),
                schema,
            ),
        )
    )


def flatten_event(df: DataFrame) -> DataFrame:
    """
    Flatten the parsed event structure.

    Keeps Kafka metadata while expanding the event payload.
    """

    return df.select(
        "topic",
        "partition",
        "offset",
        "kafka_timestamp",
        col("event.timestamp").alias("event_timestamp"),
        col("event.payload.*"),
    )