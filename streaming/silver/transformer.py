"""
Silver layer transformations.

Responsibility:
- Standardize structured event data.
- Convert timestamp fields to Spark timestamps.
- Preserve Kafka metadata.

No schema definitions.
No parsing logic.
No data quality validation.
No business aggregations.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_timestamp


def transform_event(df: DataFrame) -> DataFrame:
    """
    Apply common Silver transformations to a parsed event DataFrame.

    Expected input:
        DataFrame produced by parser.flatten_event()

    Returns:
        Cleaned Silver-ready DataFrame.
    """

    transformed_df = df

    # -------------------------------------------------------------
    # Event timestamp
    # -------------------------------------------------------------

    if "event_timestamp" in transformed_df.columns:
        transformed_df = transformed_df.withColumn(
            "event_timestamp",
            to_timestamp(col("event_timestamp")),
        )

    # -------------------------------------------------------------
    # Kafka timestamp
    # -------------------------------------------------------------

    if "kafka_timestamp" in transformed_df.columns:
        transformed_df = transformed_df.withColumn(
            "kafka_timestamp",
            to_timestamp(col("kafka_timestamp")),
        )

    # -------------------------------------------------------------
    # Common business timestamps
    # -------------------------------------------------------------

    timestamp_columns = [
        "created_at",
        "updated_at",
        "initiated_at",
        "completed_at",
        "requested_at",
        "resolved_at",
        "paid_at",
        "due_date",
        "onboarding_date",
    ]

    for column_name in timestamp_columns:
        if column_name in transformed_df.columns:
            transformed_df = transformed_df.withColumn(
                column_name,
                to_timestamp(col(column_name)),
            )

    return transformed_df