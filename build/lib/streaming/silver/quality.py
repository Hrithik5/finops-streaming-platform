"""
Silver layer data quality checks.

Responsibility:
- Validate structured Silver-ready DataFrames.
- Detect null required identifiers.
- Detect negative monetary values.
- Detect duplicate Kafka records.

No parsing logic.
No transformations.
No writes.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def check_required_columns(
    df: DataFrame,
    required_columns: list[str],
) -> dict[str, int]:
    """
    Count null values in required columns.
    """

    results = {}

    for column_name in required_columns:
        if column_name not in df.columns:
            results[column_name] = -1
            continue

        results[column_name] = df.filter(
            col(column_name).isNull()
        ).count()

    return results


def check_negative_values(
    df: DataFrame,
    amount_columns: list[str],
) -> dict[str, int]:
    """
    Count negative values in monetary/numeric columns.
    """

    results = {}

    for column_name in amount_columns:
        if column_name not in df.columns:
            continue

        results[column_name] = df.filter(
            col(column_name) < 0
        ).count()

    return results


def check_duplicate_events(
    df: DataFrame,
) -> int:
    """
    Detect duplicate Kafka records using topic,
    partition, and offset.
    """

    total_count = df.count()

    distinct_count = (
        df.select(
            "topic",
            "partition",
            "offset",
        )
        .distinct()
        .count()
    )

    return total_count - distinct_count