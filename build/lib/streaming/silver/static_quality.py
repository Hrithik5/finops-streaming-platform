"""
Static Silver quality checks.

Responsibility:
- Basic null and duplicate checks for static datasets.
"""

from pyspark.sql import DataFrame


def count_duplicates(
    df: DataFrame,
    key_columns: list[str],
) -> int:
    """
    Count duplicate rows based on the supplied key columns.
    """

    total_count = df.count()

    distinct_count = (
        df.select(*key_columns)
        .distinct()
        .count()
    )

    return total_count - distinct_count