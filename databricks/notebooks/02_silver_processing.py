"""
02_silver_processing.py

Silver processing pipeline.

Flow:
    Bronze
      ↓
    Parse
      ↓
    Flatten
      ↓
    Transform
      ↓
    Data Quality Validation
      ↓
    Silver
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
# Read Bronze
# ---------------------------------------------------------------------

bronze_df = spark.read.table(
    "dev.bronze.raw_events"
)


# ---------------------------------------------------------------------
# Required Columns by Topic
# ---------------------------------------------------------------------

required_columns_by_topic = {
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
# Process Each Topic
# ---------------------------------------------------------------------

for topic in EVENT_SCHEMAS:

    # -------------------------------------------------------------
    # Parse
    # -------------------------------------------------------------

    parsed_df = parse_event(
        bronze_df,
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

    required_columns = required_columns_by_topic[
        topic
    ]

    null_results = check_required_columns(
        transformed_df,
        required_columns,
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

    # -------------------------------------------------------------
    # Fail Job on Critical Data Quality Issues
    # -------------------------------------------------------------

    if duplicates > 0:
        raise ValueError(
            f"{topic}: {duplicates} duplicate events detected."
        )

    invalid_required_columns = {
        column_name: count
        for column_name, count in null_results.items()
        if count > 0
    }

    if invalid_required_columns:
        raise ValueError(
            f"{topic}: required column validation failed: "
            f"{invalid_required_columns}"
        )

    invalid_negative_values = {
        column_name: count
        for column_name, count in negative_results.items()
        if count > 0
    }

    if invalid_negative_values:
        raise ValueError(
            f"{topic}: negative value validation failed: "
            f"{invalid_negative_values}"
        )

    # -------------------------------------------------------------
    # Write Silver
    # -------------------------------------------------------------

    table_name = topic_to_table_name(topic)

    write_silver(
        transformed_df,
        topic,
    )

    print(
        f"Completed Silver processing: "
        f"dev.silver.{table_name}"
    )