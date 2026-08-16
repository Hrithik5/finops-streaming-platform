"""
02_silver_processing.py

Incremental Silver processing pipeline.

Flow:
    Bronze Delta Stream
          ↓
    foreachBatch
          ↓
        Parse
          ↓
       Flatten
          ↓
      Transform
          ↓
     Data Quality
          ↓
    Idempotent Silver Merge
          ↓
      Checkpoint
"""

from streaming.silver.parser import (
    flatten_event,
    parse_event,
)
from streaming.silver.quality import (
    check_duplicate_events,
    check_negative_values,
    check_required_columns,
)
from streaming.silver.schemas import EVENT_SCHEMAS
from streaming.silver.transformer import transform_event
from streaming.silver.writer import (
    topic_to_table_name,
    write_silver,
)


# ---------------------------------------------------------------------
# Required Columns by Topic
# ---------------------------------------------------------------------

REQUIRED_COLUMNS_BY_TOPIC = {
    "chargeback-events": [
        "chargeback_id",
        "merchant_id",
        "payment_id",
    ],
    "customer-events": [
        "customer_id",
    ],
    "invoice-events": [
        "customer_id",
        "invoice_id",
        "merchant_id",
    ],
    "merchant-events": [
        "merchant_id",
    ],
    "payment-events": [
        "payment_id",
        "merchant_id",
        "customer_id",
        "gateway_id",
        "payment_method_id",
    ],
    "refund-events": [
        "refund_id",
        "merchant_id",
        "payment_id",
    ],
    "settlement-events": [
        "settlement_id",
        "account_id",
        "merchant_id",
        "payment_id",
    ],
}


# ---------------------------------------------------------------------
# Silver Microbatch Processor
# ---------------------------------------------------------------------


def process_silver_batch(
    batch_df,
    batch_id: int,
) -> None:
    """
    Process one incremental Bronze microbatch.

    Each batch contains only newly available Bronze records
    according to the Bronze streaming checkpoint.
    """

    if batch_df.isEmpty():
        return

    for topic in EVENT_SCHEMAS:

        # -------------------------------------------------------------
        # Parse
        # -------------------------------------------------------------

        parsed_df = parse_event(
            batch_df,
            topic,
        )

        # -------------------------------------------------------------
        # Flatten
        # -------------------------------------------------------------

        flattened_df = flatten_event(
            parsed_df,
        )

        # -------------------------------------------------------------
        # Transform
        # -------------------------------------------------------------

        transformed_df = transform_event(
            flattened_df,
        )

        # -------------------------------------------------------------
        # Data Quality Validation
        # -------------------------------------------------------------

        duplicates = check_duplicate_events(
            transformed_df,
        )

        if duplicates > 0:
            raise ValueError(
                f"{topic}: "
                f"{duplicates} duplicate events detected "
                f"in batch {batch_id}."
            )

        required_columns = (
            REQUIRED_COLUMNS_BY_TOPIC[topic]
        )

        null_results = check_required_columns(
            transformed_df,
            required_columns,
        )

        invalid_required_columns = {
            column_name: count
            for column_name, count in null_results.items()
            if count > 0
        }

        if invalid_required_columns:
            raise ValueError(
                f"{topic}: required column validation failed "
                f"in batch {batch_id}: "
                f"{invalid_required_columns}"
            )

        amount_columns = [
            column_name
            for column_name in [
                "amount",
                "invoice_amount",
                "refund_amount",
                "settlement_amount",
            ]
            if column_name in transformed_df.columns
        ]

        negative_results = check_negative_values(
            transformed_df,
            amount_columns,
        )

        invalid_negative_values = {
            column_name: count
            for column_name, count in negative_results.items()
            if count > 0
        }

        if invalid_negative_values:
            raise ValueError(
                f"{topic}: negative value validation failed "
                f"in batch {batch_id}: "
                f"{invalid_negative_values}"
            )

        # -------------------------------------------------------------
        # Incremental Silver Write
        # -------------------------------------------------------------

        write_silver(
            transformed_df,
            topic,
        )

        table_name = topic_to_table_name(
            topic
        )

        print(
            f"Batch {batch_id}: "
            f"completed {topic} → "
            f"dev.silver.{table_name}"
        )


# ---------------------------------------------------------------------
# Read Bronze Incrementally
# ---------------------------------------------------------------------

bronze_stream_df = (
    spark.readStream
    .table("dev.bronze.raw_events")
)


# ---------------------------------------------------------------------
# Start Silver Streaming Query
# ---------------------------------------------------------------------

silver_query = (
    bronze_stream_df.writeStream
    .foreachBatch(process_silver_batch)
    .option(
        "checkpointLocation",
        "/Volumes/dev/stream/streaming_checkpoints/silver",
    )
    .trigger(availableNow=True)
    .start()
)


# ---------------------------------------------------------------------
# Wait for Completion
# ---------------------------------------------------------------------

silver_query.awaitTermination()

print(
    "Incremental Silver processing completed successfully."
)