"""
Static Silver transformations.

Responsibility:
- Apply common type normalization to static datasets.

No Kafka logic.
No business aggregations.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_timestamp


TIMESTAMP_COLUMNS = [
    "created_at",
    "updated_at",
    "initiated_at",
    "completed_at",
    "requested_at",
    "resolved_at",
    "onboarding_date",
]


def transform_static(df: DataFrame) -> DataFrame:
    """
    Apply common timestamp normalization.
    """

    transformed_df = df

    for column_name in TIMESTAMP_COLUMNS:
        if column_name in transformed_df.columns:
            transformed_df = transformed_df.withColumn(
                column_name,
                to_timestamp(col(column_name)),
            )

    return transformed_df