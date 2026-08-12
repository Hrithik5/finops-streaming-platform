"""
03_gold_processing.py

Gold layer pipeline.

Gold datasets:
1. payment_performance      -> one row per payment
2. merchant_performance     -> one row per merchant
3. gateway_performance      -> one row per gateway
4. financial_operations     -> one row per day
"""

from streaming.gold.metrics import (
    build_financial_operations,
    build_gateway_performance,
    build_merchant_performance,
    build_payment_performance,
)


# =====================================================================
# Read Silver Tables
# =====================================================================

payments_df = spark.read.table(
    "dev.silver.payments"
)

merchants_df = spark.read.table(
    "dev.silver.merchants"
)

customers_df = spark.read.table(
    "dev.silver.customers"
)

invoices_df = spark.read.table(
    "dev.silver.invoices"
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


payment_rows = payment_performance_df.count()

payment_distinct = (
    payment_performance_df
    .select("payment_id")
    .distinct()
    .count()
)

print("=" * 70)
print("PAYMENT PERFORMANCE")
print("=" * 70)
print(f"Rows              : {payment_rows}")
print(f"Distinct payments : {payment_distinct}")
print(
    f"Duplicate rows    : "
    f"{payment_rows - payment_distinct}"
)

display(
    payment_performance_df.limit(20)
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


merchant_rows = merchant_performance_df.count()

merchant_distinct = (
    merchant_performance_df
    .select("merchant_id")
    .distinct()
    .count()
)

print("=" * 70)
print("MERCHANT PERFORMANCE")
print("=" * 70)
print(f"Rows               : {merchant_rows}")
print(f"Distinct merchants : {merchant_distinct}")
print(
    f"Duplicate rows     : "
    f"{merchant_rows - merchant_distinct}"
)

display(
    merchant_performance_df.limit(20)
)


# =====================================================================
# Gateway Performance
# =====================================================================

gateway_performance_df = build_gateway_performance(
    gateways_df=gateways_df,
    payments_df=payments_df,
    attempts_df=attempts_df,
)


gateway_rows = gateway_performance_df.count()

gateway_distinct = (
    gateway_performance_df
    .select("gateway_id")
    .distinct()
    .count()
)

print("=" * 70)
print("GATEWAY PERFORMANCE")
print("=" * 70)
print(f"Rows              : {gateway_rows}")
print(f"Distinct gateways : {gateway_distinct}")
print(
    f"Duplicate rows    : "
    f"{gateway_rows - gateway_distinct}"
)

display(
    gateway_performance_df.limit(20)
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


financial_rows = financial_operations_df.count()

financial_distinct = (
    financial_operations_df
    .select("operation_date")
    .distinct()
    .count()
)

print("=" * 70)
print("FINANCIAL OPERATIONS")
print("=" * 70)
print(f"Rows               : {financial_rows}")
print(f"Distinct dates     : {financial_distinct}")
print(
    f"Duplicate rows     : "
    f"{financial_rows - financial_distinct}"
)

display(
    financial_operations_df.orderBy(
        "operation_date"
    )
)


# =====================================================================
# Write Gold Tables
# =====================================================================

(
    payment_performance_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("dev.gold.payment_performance")
)

(
    merchant_performance_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("dev.gold.merchant_performance")
)

(
    gateway_performance_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("dev.gold.gateway_performance")
)

(
    financial_operations_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("dev.gold.financial_operations")
)


# =====================================================================
# Completion
# =====================================================================

print("=" * 70)
print("GOLD LAYER COMPLETE")
print("=" * 70)

print(
    "dev.gold.payment_performance"
)
print(
    "dev.gold.merchant_performance"
)
print(
    "dev.gold.gateway_performance"
)
print(
    "dev.gold.financial_operations"
)