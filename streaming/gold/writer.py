"""
Gold layer writer.

Responsibility:
- Write incremental Gold microbatches to Delta.
- Merge by business key.

No transformations.
No joins.
No business logic.
"""

from delta.tables import DeltaTable
from pyspark.sql import DataFrame


def write_gold_incremental(
    df: DataFrame,
    table_name: str,
    key_columns: list[str],
) -> None:
    """
    Incrementally merge a Gold microbatch into a Delta table.
    """

    target = DeltaTable.forName(
        df.sparkSession,
        table_name,
    )

    merge_condition = " AND ".join(
        f"target.{column} = source.{column}" for column in key_columns
    )

    (
        target.alias("target")
        .merge(
            df.alias("source"),
            merge_condition,
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
