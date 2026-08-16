"""
Incremental Gold metrics.

Responsibility:
- Rebuild only affected Gold records.
- Read the current Silver state for affected keys.
- Reuse the validated full-refresh Gold builders.

No writing.
No orchestration.
"""

from pyspark.sql import DataFrame


# =====================================================================
# Payment Performance
# =====================================================================


def build_incremental_payment_performance(
    payment_ids_df: DataFrame,
    payments_df: DataFrame,
    merchants_df: DataFrame,
    gateways_df: DataFrame,
    payment_methods_df: DataFrame,
    attempts_df: DataFrame,
    events_df: DataFrame,
    refunds_df: DataFrame,
    chargebacks_df: DataFrame,
    settlements_df: DataFrame,
    build_payment_performance_func,
) -> DataFrame:

    payment_ids_df = (
        payment_ids_df
        .select("payment_id")
        .where("payment_id IS NOT NULL")
        .distinct()
    )

    return build_payment_performance_func(
        payments_df=payments_df.join(
            payment_ids_df,
            "payment_id",
            "inner",
        ),
        merchants_df=merchants_df,
        gateways_df=gateways_df,
        payment_methods_df=payment_methods_df,
        attempts_df=attempts_df.join(
            payment_ids_df,
            "payment_id",
            "inner",
        ),
        events_df=events_df.join(
            payment_ids_df,
            "payment_id",
            "inner",
        ),
        refunds_df=refunds_df.join(
            payment_ids_df,
            "payment_id",
            "inner",
        ),
        chargebacks_df=chargebacks_df.join(
            payment_ids_df,
            "payment_id",
            "inner",
        ),
        settlements_df=settlements_df.join(
            payment_ids_df,
            "payment_id",
            "inner",
        ),
    )


# =====================================================================
# Merchant Performance
# =====================================================================


def build_incremental_merchant_performance(
    merchant_ids_df: DataFrame,
    merchants_df: DataFrame,
    payments_df: DataFrame,
    refunds_df: DataFrame,
    chargebacks_df: DataFrame,
    settlements_df: DataFrame,
    build_merchant_performance_func,
) -> DataFrame:

    merchant_ids_df = (
        merchant_ids_df
        .select("merchant_id")
        .where("merchant_id IS NOT NULL")
        .distinct()
    )

    return build_merchant_performance_func(
        merchants_df=merchants_df.join(
            merchant_ids_df,
            "merchant_id",
            "inner",
        ),
        payments_df=payments_df.join(
            merchant_ids_df,
            "merchant_id",
            "inner",
        ),
        refunds_df=refunds_df.join(
            merchant_ids_df,
            "merchant_id",
            "inner",
        ),
        chargebacks_df=chargebacks_df.join(
            merchant_ids_df,
            "merchant_id",
            "inner",
        ),
        settlements_df=settlements_df.join(
            merchant_ids_df,
            "merchant_id",
            "inner",
        ),
    )


# =====================================================================
# Gateway Performance
# =====================================================================


def build_incremental_gateway_performance(
    gateway_ids_df: DataFrame,
    gateways_df: DataFrame,
    payments_df: DataFrame,
    attempts_df: DataFrame,
    build_gateway_performance_func,
) -> DataFrame:

    gateway_ids_df = (
        gateway_ids_df
        .select("gateway_id")
        .where("gateway_id IS NOT NULL")
        .distinct()
    )

    return build_gateway_performance_func(
        gateways_df=gateways_df.join(
            gateway_ids_df,
            "gateway_id",
            "inner",
        ),
        payments_df=payments_df.join(
            gateway_ids_df,
            "gateway_id",
            "inner",
        ),
        attempts_df=attempts_df.join(
            gateway_ids_df,
            "gateway_id",
            "inner",
        ),
    )


# =====================================================================
# Financial Operations
# =====================================================================


def build_incremental_financial_operations(
    operation_dates_df: DataFrame,
    payments_df: DataFrame,
    refunds_df: DataFrame,
    chargebacks_df: DataFrame,
    settlements_df: DataFrame,
    build_financial_operations_func,
) -> DataFrame:

    operation_dates_df = (
        operation_dates_df
        .select("operation_date")
        .where("operation_date IS NOT NULL")
        .distinct()
    )

    from pyspark.sql.functions import coalesce, col, to_date

    payments_filtered = (
        payments_df
        .withColumn(
            "operation_date",
            to_date(
                coalesce(
                    col("completed_at"),
                    col("initiated_at"),
                )
            ),
        )
        .join(
            operation_dates_df,
            "operation_date",
            "inner",
        )
        .drop("operation_date")
    )

    refunds_filtered = (
        refunds_df
        .withColumn(
            "operation_date",
            to_date(
                coalesce(
                    col("completed_at"),
                    col("requested_at"),
                )
            ),
        )
        .join(
            operation_dates_df,
            "operation_date",
            "inner",
        )
        .drop("operation_date")
    )

    chargebacks_filtered = (
        chargebacks_df
        .withColumn(
            "operation_date",
            to_date(col("created_at")),
        )
        .join(
            operation_dates_df,
            "operation_date",
            "inner",
        )
        .drop("operation_date")
    )

    settlements_filtered = (
        settlements_df
        .withColumn(
            "operation_date",
            to_date(
                coalesce(
                    col("completed_at"),
                    col("initiated_at"),
                )
            ),
        )
        .join(
            operation_dates_df,
            "operation_date",
            "inner",
        )
        .drop("operation_date")
    )

    return build_financial_operations_func(
        payments_df=payments_filtered,
        refunds_df=refunds_filtered,
        chargebacks_df=chargebacks_filtered,
        settlements_df=settlements_filtered,
    )