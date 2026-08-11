"""
Silver layer schemas.

Contains explicit Spark schemas for all FinOps event types.
"""

from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
)


# ---------------------------------------------------------------------
# Chargeback
# ---------------------------------------------------------------------

CHARGEBACK_SCHEMA = StructType(
    [
        StructField("timestamp", StringType(), True),
        StructField(
            "payload",
            StructType(
                [
                    StructField("amount", LongType(), True),
                    StructField("chargeback_id", StringType(), True),
                    StructField("chargeback_reason", StringType(), True),
                    StructField("created_at", StringType(), True),
                    StructField("merchant_id", StringType(), True),
                    StructField("payment_id", StringType(), True),
                    StructField("resolved_at", StringType(), True),
                    StructField("status", StringType(), True),
                ]
            ),
            True,
        ),
    ]
)


# ---------------------------------------------------------------------
# Customer
# ---------------------------------------------------------------------

CUSTOMER_SCHEMA = StructType(
    [
        StructField("timestamp", StringType(), True),
        StructField(
            "payload",
            StructType(
                [
                    StructField("city", StringType(), True),
                    StructField("country", StringType(), True),
                    StructField("created_at", StringType(), True),
                    StructField("customer_id", StringType(), True),
                    StructField("customer_name", StringType(), True),
                    StructField("email", StringType(), True),
                    StructField("phone", LongType(), True),
                ]
            ),
            True,
        ),
    ]
)


# ---------------------------------------------------------------------
# Invoice
# ---------------------------------------------------------------------

INVOICE_SCHEMA = StructType(
    [
        StructField("timestamp", StringType(), True),
        StructField(
            "payload",
            StructType(
                [
                    StructField("created_at", StringType(), True),
                    StructField("currency", StringType(), True),
                    StructField("customer_id", StringType(), True),
                    StructField("due_date", StringType(), True),
                    StructField("invoice_amount", LongType(), True),
                    StructField("invoice_id", StringType(), True),
                    StructField("invoice_number", StringType(), True),
                    StructField("invoice_status", StringType(), True),
                    StructField("merchant_id", StringType(), True),
                    StructField("paid_at", StringType(), True),
                ]
            ),
            True,
        ),
    ]
)


# ---------------------------------------------------------------------
# Merchant
# ---------------------------------------------------------------------

MERCHANT_SCHEMA = StructType(
    [
        StructField("timestamp", StringType(), True),
        StructField(
            "payload",
            StructType(
                [
                    StructField("business_type", StringType(), True),
                    StructField("city", StringType(), True),
                    StructField("country", StringType(), True),
                    StructField("created_at", StringType(), True),
                    StructField("email", StringType(), True),
                    StructField("industry", StringType(), True),
                    StructField("merchant_id", StringType(), True),
                    StructField("merchant_name", StringType(), True),
                    StructField("onboarding_date", StringType(), True),
                    StructField("phone", LongType(), True),
                    StructField("risk_level", StringType(), True),
                    StructField("settlement_cycle", StringType(), True),
                    StructField("state", StringType(), True),
                    StructField("updated_at", StringType(), True),
                    StructField("verification_status", StringType(), True),
                ]
            ),
            True,
        ),
    ]
)


# ---------------------------------------------------------------------
# Payment
# ---------------------------------------------------------------------

PAYMENT_SCHEMA = StructType(
    [
        StructField("timestamp", StringType(), True),
        StructField(
            "payload",
            StructType(
                [
                    StructField("amount", LongType(), True),
                    StructField("completed_at", StringType(), True),
                    StructField("currency", StringType(), True),
                    StructField("customer_id", StringType(), True),
                    StructField("gateway_id", StringType(), True),
                    StructField("initiated_at", StringType(), True),
                    StructField("invoice_id", StringType(), True),
                    StructField("merchant_id", StringType(), True),
                    StructField("payment_id", StringType(), True),
                    StructField("payment_method_id", StringType(), True),
                    StructField("payment_reference", StringType(), True),
                    StructField("payment_status", StringType(), True),
                    StructField("processing_time_ms", LongType(), True),
                ]
            ),
            True,
        ),
    ]
)


# ---------------------------------------------------------------------
# Refund
# ---------------------------------------------------------------------

REFUND_SCHEMA = StructType(
    [
        StructField("timestamp", StringType(), True),
        StructField(
            "payload",
            StructType(
                [
                    StructField("completed_at", StringType(), True),
                    StructField("merchant_id", StringType(), True),
                    StructField("payment_id", StringType(), True),
                    StructField("refund_amount", LongType(), True),
                    StructField("refund_id", StringType(), True),
                    StructField("refund_reason", StringType(), True),
                    StructField("refund_status", StringType(), True),
                    StructField("requested_at", StringType(), True),
                ]
            ),
            True,
        ),
    ]
)


# ---------------------------------------------------------------------
# Settlement
# ---------------------------------------------------------------------

SETTLEMENT_SCHEMA = StructType(
    [
        StructField("timestamp", StringType(), True),
        StructField(
            "payload",
            StructType(
                [
                    StructField("account_id", StringType(), True),
                    StructField("bank_reference", StringType(), True),
                    StructField("completed_at", StringType(), True),
                    StructField("initiated_at", StringType(), True),
                    StructField("merchant_id", StringType(), True),
                    StructField("payment_id", StringType(), True),
                    StructField("settlement_amount", LongType(), True),
                    StructField("settlement_id", StringType(), True),
                    StructField("settlement_status", StringType(), True),
                ]
            ),
            True,
        ),
    ]
)


# ---------------------------------------------------------------------
# Schema Registry
# ---------------------------------------------------------------------

EVENT_SCHEMAS = {
    "chargeback-events": CHARGEBACK_SCHEMA,
    "customer-events": CUSTOMER_SCHEMA,
    "invoice-events": INVOICE_SCHEMA,
    "merchant-events": MERCHANT_SCHEMA,
    "payment-events": PAYMENT_SCHEMA,
    "refund-events": REFUND_SCHEMA,
    "settlement-events": SETTLEMENT_SCHEMA,
}