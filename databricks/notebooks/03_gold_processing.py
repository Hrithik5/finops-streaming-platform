"""
03_gold_processing.py

Gold layer pipeline.

Gold datasets:
1. payment_performance  -> one row per payment
2. merchant_performance -> one row per merchant
3. gateway_performance  -> one row per gateway
4. financial_operations -> one row per day
"""

from streaming.gold.metrics import (
    build_financial_operations,
    build_gateway_performance,
    build_merchant_performance,
    build_payment_performance,
)
from streaming.gold.writer import write_gold


# =====================================================================
# Read Silver Tables
# =====================================================================

payments_df = spark.read.table(
    "dev.silver.payments"
)

merchants_df = spark.read.table(
    "dev.silver.merchants"
)

gateways_df = spark.read.table(
    "dev.silver.payment_gateways"
)

payment_methods_df = spark.read.table(
    "dev.silver.payment_methods"
)

attempts_df = spark.read.table(
    "dev.silver.payment_attempts"
)

events_df = spark.read.table(
    "dev.silver.payment_events"
)

refunds_df = spark.read.table(
    "dev.silver.refunds"
)

chargebacks_df = spark.read.table(
    "dev.silver.chargebacks"
)

settlements_df = spark.read.table(
    "dev.silver.settlements"
)


# =====================================================================
# Payment Performance
# =====================================================================

payment_performance_df = build_payment_performance(
    payments_df=payments_df,
    merchants_df=merchants_df,
    gateways_df=gateways_df,
    payment_methods_df=payment_methods_df,
    attempts_df=attempts_df,
    events_df=events_df,
    refunds_df=refunds_df,
    chargebacks_df=chargebacks_df,
    settlements_df=settlements_df,
)

payment_duplicate_count = (
    payment_performance_df.count()
    - payment_performance_df.select("payment_id").distinct().count()
)

if payment_duplicate_count > 0:
    raise ValueError(
        "payment_performance contains "
        f"{payment_duplicate_count} duplicate payment records."
    )


# =====================================================================
# Merchant Performance
# =====================================================================

merchant_performance_df = build_merchant_performance(
    merchants_df=merchants_df,
    payments_df=payments_df,
    refunds_df=refunds_df,
    chargebacks_df=chargebacks_df,
    settlements_df=settlements_df,
)

merchant_duplicate_count = (
    merchant_performance_df.count()
    - merchant_performance_df.select("merchant_id").distinct().count()
)

if merchant_duplicate_count > 0:
    raise ValueError(
        "merchant_performance contains "
        f"{merchant_duplicate_count} duplicate merchant records."
    )


# =====================================================================
# Gateway Performance
# =====================================================================

gateway_performance_df = build_gateway_performance(
    gateways_df=gateways_df,
    payments_df=payments_df,
    attempts_df=attempts_df,
)

gateway_duplicate_count = (
    gateway_performance_df.count()
    - gateway_performance_df.select("gateway_id").distinct().count()
)

if gateway_duplicate_count > 0:
    raise ValueError(
        "gateway_performance contains "
        f"{gateway_duplicate_count} duplicate gateway records."
    )


# =====================================================================
# Financial Operations
# =====================================================================

financial_operations_df = build_financial_operations(
    payments_df=payments_df,
    refunds_df=refunds_df,
    chargebacks_df=chargebacks_df,
    settlements_df=settlements_df,
)

financial_duplicate_count = (
    financial_operations_df.count()
    - financial_operations_df.select("operation_date").distinct().count()
)

if financial_duplicate_count > 0:
    raise ValueError(
        "financial_operations contains "
        f"{financial_duplicate_count} duplicate date records."
    )


# =====================================================================
# Write Gold Tables
# =====================================================================

write_gold(
    payment_performance_df,
    "dev.gold.payment_performance",
)

write_gold(
    merchant_performance_df,
    "dev.gold.merchant_performance",
)

write_gold(
    gateway_performance_df,
    "dev.gold.gateway_performance",
)

write_gold(
    financial_operations_df,
    "dev.gold.financial_operations",
)


# =====================================================================
# Completion
# =====================================================================

print("Gold layer processing completed successfully.")