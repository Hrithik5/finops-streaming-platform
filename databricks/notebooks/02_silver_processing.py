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
"""

from streaming.silver.parser import (
    flatten_event,
    parse_event,
)
from streaming.silver.schemas import EVENT_SCHEMAS
from streaming.silver.transformer import transform_event
from streaming.silver.quality import (
    check_duplicate_events,
    check_negative_values,
    check_required_columns,
)
from streaming.silver.writer import write_silver

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

    print(f"\n{'=' * 60}")
    print(topic)
    print("=" * 60)

    # -------------------------------------------------------------
    # Duplicate Check
    # -------------------------------------------------------------

    duplicates = check_duplicate_events(
        transformed_df,
    )

    print(
        f"Duplicate events : {duplicates}"
    )

    # -------------------------------------------------------------
    # Required Column Check
    # -------------------------------------------------------------

    required_columns = required_columns_by_topic[
        topic
    ]

    null_results = check_required_columns(
        transformed_df,
        required_columns,
    )

    print(
        f"Required ID nulls: {null_results}"
    )

    # -------------------------------------------------------------
    # Monetary Checks
    # -------------------------------------------------------------

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

    print(
        f"Negative amounts : {negative_results}"
    )

    
    # -------------------------------------------------------------
    # Write Silver
    # -------------------------------------------------------------

    write_silver(
        transformed_df,
        topic,
    )

    print(
        f"Silver table written: "
        f"dev.silver.{topic.replace('-', '_')}"
    )