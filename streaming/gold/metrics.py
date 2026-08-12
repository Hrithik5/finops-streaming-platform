"""
Gold layer metrics.

Responsibility:
- Build business-ready Gold datasets.
- Aggregate one-to-many Silver datasets before joining.
- Preserve a clear grain for every Gold table.

Gold datasets:
1. payment_performance      -> one row per payment
2. merchant_performance     -> one row per merchant
3. gateway_performance      -> one row per gateway
4. financial_operations     -> one row per day

No writing.
No notebook orchestration.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    avg,
    coalesce,
    col,
    countDistinct,
    lit,
    max,
    sum,
    to_date,
    when,
)


# =====================================================================
# PAYMENT PERFORMANCE
# Grain: 1 row per payment
# =====================================================================


def aggregate_payment_attempts(
    attempts_df: DataFrame,
) -> DataFrame:
    """Aggregate payment attempts to one row per payment."""

    return attempts_df.groupBy("payment_id").agg(
        countDistinct("attempt_id").alias("attempt_count"),
        sum(
            when(
                col("attempt_status") == "Failed",
                1,
            ).otherwise(0)
        ).alias("failed_attempt_count"),
        sum(
            when(
                col("attempt_status") == "Success",
                1,
            ).otherwise(0)
        ).alias("successful_attempt_count"),
        max("attempt_number").alias("max_attempt_number"),
    )


def aggregate_payment_events(
    events_df: DataFrame,
) -> DataFrame:
    """Aggregate payment lifecycle events to one row per payment."""

    return events_df.groupBy("payment_id").agg(
        countDistinct("event_id").alias("event_count"),
        sum(
            coalesce(
                col("retry_count"),
                lit(0),
            )
        ).alias("total_retry_count"),
        max("event_timestamp").alias(
            "last_event_timestamp"
        ),
    )


def aggregate_refunds(
    refunds_df: DataFrame,
) -> DataFrame:
    """Aggregate refunds to one row per payment."""

    return refunds_df.groupBy("payment_id").agg(
        countDistinct("refund_id").alias("refund_count"),
        sum(
            coalesce(
                col("refund_amount"),
                lit(0),
            )
        ).alias("total_refund_amount"),
    )


def aggregate_chargebacks(
    chargebacks_df: DataFrame,
) -> DataFrame:
    """Aggregate chargebacks to one row per payment."""

    return chargebacks_df.groupBy("payment_id").agg(
        countDistinct("chargeback_id").alias(
            "chargeback_count"
        ),
        sum(
            coalesce(
                col("amount"),
                lit(0),
            )
        ).alias("total_chargeback_amount"),
    )


def aggregate_settlements(
    settlements_df: DataFrame,
) -> DataFrame:
    """Aggregate settlements to one row per payment."""

    return settlements_df.groupBy("payment_id").agg(
        countDistinct("settlement_id").alias(
            "settlement_count"
        ),
        sum(
            coalesce(
                col("settlement_amount"),
                lit(0),
            )
        ).alias("total_settlement_amount"),
    )


def build_payment_performance(
    payments_df: DataFrame,
    merchants_df: DataFrame,
    gateways_df: DataFrame,
    payment_methods_df: DataFrame,
    attempts_df: DataFrame,
    events_df: DataFrame,
    refunds_df: DataFrame,
    chargebacks_df: DataFrame,
    settlements_df: DataFrame,
) -> DataFrame:
    """
    Build one-row-per-payment Gold dataset.

    Child datasets are aggregated before joining to prevent
    row multiplication.
    """

    attempts_agg = aggregate_payment_attempts(attempts_df)
    events_agg = aggregate_payment_events(events_df)
    refunds_agg = aggregate_refunds(refunds_df)
    chargebacks_agg = aggregate_chargebacks(chargebacks_df)
    settlements_agg = aggregate_settlements(settlements_df)

    return (
        payments_df.alias("p")
        .join(
            merchants_df.alias("m"),
            on="merchant_id",
            how="left",
        )
        .join(
            gateways_df.alias("g"),
            on="gateway_id",
            how="left",
        )
        .join(
            payment_methods_df.alias("pm"),
            on="payment_method_id",
            how="left",
        )
        .join(
            attempts_agg,
            on="payment_id",
            how="left",
        )
        .join(
            events_agg,
            on="payment_id",
            how="left",
        )
        .join(
            refunds_agg,
            on="payment_id",
            how="left",
        )
        .join(
            chargebacks_agg,
            on="payment_id",
            how="left",
        )
        .join(
            settlements_agg,
            on="payment_id",
            how="left",
        )
        .select(
            "p.payment_id",
            "p.merchant_id",
            "p.customer_id",
            "p.invoice_id",
            "p.gateway_id",
            "p.payment_method_id",
            "p.amount",
            "p.currency",
            "p.payment_status",
            "p.payment_reference",
            "p.initiated_at",
            "p.completed_at",
            "p.processing_time_ms",
            "m.merchant_name",
            "m.industry",
            "m.risk_level",
            "g.gateway_name",
            "g.provider",
            "g.gateway_type",
            "pm.payment_method_name",
            "pm.category",
            coalesce(
                col("attempt_count"),
                lit(0),
            ).alias("attempt_count"),
            coalesce(
                col("failed_attempt_count"),
                lit(0),
            ).alias("failed_attempt_count"),
            coalesce(
                col("successful_attempt_count"),
                lit(0),
            ).alias("successful_attempt_count"),
            coalesce(
                col("max_attempt_number"),
                lit(0),
            ).alias("max_attempt_number"),
            coalesce(
                col("event_count"),
                lit(0),
            ).alias("event_count"),
            coalesce(
                col("total_retry_count"),
                lit(0),
            ).alias("total_retry_count"),
            col("last_event_timestamp"),
            coalesce(
                col("refund_count"),
                lit(0),
            ).alias("refund_count"),
            coalesce(
                col("total_refund_amount"),
                lit(0),
            ).alias("total_refund_amount"),
            coalesce(
                col("chargeback_count"),
                lit(0),
            ).alias("chargeback_count"),
            coalesce(
                col("total_chargeback_amount"),
                lit(0),
            ).alias("total_chargeback_amount"),
            coalesce(
                col("settlement_count"),
                lit(0),
            ).alias("settlement_count"),
            coalesce(
                col("total_settlement_amount"),
                lit(0),
            ).alias("total_settlement_amount"),
        )
    )


# =====================================================================
# MERCHANT PERFORMANCE
# Grain: 1 row per merchant
# =====================================================================


def aggregate_payments_by_merchant(
    payments_df: DataFrame,
) -> DataFrame:
    """Aggregate payments to one row per merchant."""

    return payments_df.groupBy("merchant_id").agg(
        countDistinct("payment_id").alias("payment_count"),
        sum(
            coalesce(
                col("amount"),
                lit(0),
            )
        ).alias("total_payment_amount"),
        sum(
            when(
                col("payment_status") == "Captured",
                1,
            ).otherwise(0)
        ).alias("successful_payment_count"),
        sum(
            when(
                col("payment_status") != "Captured",
                1,
            ).otherwise(0)
        ).alias("non_successful_payment_count"),
        sum(
            when(
                col("payment_status") == "Captured",
                col("amount"),
            ).otherwise(0)
        ).alias("successful_payment_amount"),
        avg("processing_time_ms").alias(
            "avg_processing_time_ms"
        ),
        max("completed_at").alias(
            "last_payment_completed_at"
        ),
    )


def aggregate_refunds_by_merchant(
    refunds_df: DataFrame,
) -> DataFrame:
    """Aggregate refunds to one row per merchant."""

    return refunds_df.groupBy("merchant_id").agg(
        countDistinct("refund_id").alias("refund_count"),
        sum(
            coalesce(
                col("refund_amount"),
                lit(0),
            )
        ).alias("total_refund_amount"),
    )


def aggregate_chargebacks_by_merchant(
    chargebacks_df: DataFrame,
) -> DataFrame:
    """Aggregate chargebacks to one row per merchant."""

    return chargebacks_df.groupBy("merchant_id").agg(
        countDistinct("chargeback_id").alias(
            "chargeback_count"
        ),
        sum(
            coalesce(
                col("amount"),
                lit(0),
            )
        ).alias("total_chargeback_amount"),
    )


def aggregate_settlements_by_merchant(
    settlements_df: DataFrame,
) -> DataFrame:
    """Aggregate settlements to one row per merchant."""

    return settlements_df.groupBy("merchant_id").agg(
        countDistinct("settlement_id").alias(
            "settlement_count"
        ),
        sum(
            coalesce(
                col("settlement_amount"),
                lit(0),
            )
        ).alias("total_settlement_amount"),
    )


def build_merchant_performance(
    merchants_df: DataFrame,
    payments_df: DataFrame,
    refunds_df: DataFrame,
    chargebacks_df: DataFrame,
    settlements_df: DataFrame,
) -> DataFrame:
    """Build one-row-per-merchant Gold dataset."""

    payments_agg = aggregate_payments_by_merchant(
        payments_df
    )

    refunds_agg = aggregate_refunds_by_merchant(
        refunds_df
    )

    chargebacks_agg = aggregate_chargebacks_by_merchant(
        chargebacks_df
    )

    settlements_agg = aggregate_settlements_by_merchant(
        settlements_df
    )

    return (
        merchants_df.alias("m")
        .join(
            payments_agg,
            on="merchant_id",
            how="left",
        )
        .join(
            refunds_agg,
            on="merchant_id",
            how="left",
        )
        .join(
            chargebacks_agg,
            on="merchant_id",
            how="left",
        )
        .join(
            settlements_agg,
            on="merchant_id",
            how="left",
        )
        .select(
            "m.merchant_id",
            "m.merchant_name",
            "m.business_type",
            "m.country",
            "m.city",
            "m.industry",
            "m.risk_level",
            "m.settlement_cycle",
            "m.verification_status",
            coalesce(
                col("payment_count"),
                lit(0),
            ).alias("payment_count"),
            coalesce(
                col("total_payment_amount"),
                lit(0),
            ).alias("total_payment_amount"),
            coalesce(
                col("successful_payment_count"),
                lit(0),
            ).alias("successful_payment_count"),
            coalesce(
                col("non_successful_payment_count"),
                lit(0),
            ).alias("non_successful_payment_count"),
            coalesce(
                col("successful_payment_amount"),
                lit(0),
            ).alias("successful_payment_amount"),
            coalesce(
                col("avg_processing_time_ms"),
                lit(0),
            ).alias("avg_processing_time_ms"),
            coalesce(
                col("refund_count"),
                lit(0),
            ).alias("refund_count"),
            coalesce(
                col("total_refund_amount"),
                lit(0),
            ).alias("total_refund_amount"),
            coalesce(
                col("chargeback_count"),
                lit(0),
            ).alias("chargeback_count"),
            coalesce(
                col("total_chargeback_amount"),
                lit(0),
            ).alias("total_chargeback_amount"),
            coalesce(
                col("settlement_count"),
                lit(0),
            ).alias("settlement_count"),
            coalesce(
                col("total_settlement_amount"),
                lit(0),
            ).alias("total_settlement_amount"),
            col("last_payment_completed_at"),
        )
    )


# =====================================================================
# GATEWAY PERFORMANCE
# Grain: 1 row per gateway
# =====================================================================


def aggregate_gateway_payments(
    payments_df: DataFrame,
) -> DataFrame:
    """Aggregate payments to one row per gateway."""

    return payments_df.groupBy("gateway_id").agg(
        countDistinct("payment_id").alias("payment_count"),
        sum(
            coalesce(
                col("amount"),
                lit(0),
            )
        ).alias("total_payment_amount"),
        sum(
            when(
                col("payment_status") == "Captured",
                1,
            ).otherwise(0)
        ).alias("successful_payment_count"),
        sum(
            when(
                col("payment_status") != "Captured",
                1,
            ).otherwise(0)
        ).alias("failed_or_other_payment_count"),
        avg("processing_time_ms").alias(
            "avg_processing_time_ms"
        ),
    )


def aggregate_gateway_attempts(
    attempts_df: DataFrame,
) -> DataFrame:
    """Aggregate payment attempts to one row per gateway."""

    return attempts_df.groupBy("gateway_id").agg(
        countDistinct("attempt_id").alias("attempt_count"),
        sum(
            when(
                col("attempt_status") == "Failed",
                1,
            ).otherwise(0)
        ).alias("failed_attempt_count"),
        sum(
            when(
                col("attempt_status") == "Success",
                1,
            ).otherwise(0)
        ).alias("successful_attempt_count"),
        max("attempt_number").alias(
            "max_attempt_number"
        ),
    )


def build_gateway_performance(
    gateways_df: DataFrame,
    payments_df: DataFrame,
    attempts_df: DataFrame,
) -> DataFrame:
    """
    Build one-row-per-gateway Gold dataset.

    Gateway performance is based on payments and attempts because
    payment_events does not contain gateway_id.
    """

    payments_agg = aggregate_gateway_payments(
        payments_df
    )

    attempts_agg = aggregate_gateway_attempts(
        attempts_df
    )

    return (
        gateways_df.alias("g")
        .join(
            payments_agg,
            on="gateway_id",
            how="left",
        )
        .join(
            attempts_agg,
            on="gateway_id",
            how="left",
        )
        .select(
            "g.gateway_id",
            "g.gateway_name",
            "g.provider",
            "g.gateway_type",
            "g.status",
            coalesce(
                col("payment_count"),
                lit(0),
            ).alias("payment_count"),
            coalesce(
                col("total_payment_amount"),
                lit(0),
            ).alias("total_payment_amount"),
            coalesce(
                col("successful_payment_count"),
                lit(0),
            ).alias("successful_payment_count"),
            coalesce(
                col("failed_or_other_payment_count"),
                lit(0),
            ).alias(
                "failed_or_other_payment_count"
            ),
            coalesce(
                col("avg_processing_time_ms"),
                lit(0),
            ).alias("avg_processing_time_ms"),
            coalesce(
                col("attempt_count"),
                lit(0),
            ).alias("attempt_count"),
            coalesce(
                col("failed_attempt_count"),
                lit(0),
            ).alias("failed_attempt_count"),
            coalesce(
                col("successful_attempt_count"),
                lit(0),
            ).alias("successful_attempt_count"),
            coalesce(
                col("max_attempt_number"),
                lit(0),
            ).alias("max_attempt_number"),
        )
    )


# =====================================================================
# FINANCIAL OPERATIONS
# Grain: 1 row per day
# =====================================================================


def build_financial_operations(
    payments_df: DataFrame,
    refunds_df: DataFrame,
    chargebacks_df: DataFrame,
    settlements_df: DataFrame,
) -> DataFrame:
    """
    Build one-row-per-day financial operations summary.

    Daily metrics:
    - payment volume
    - captured payment volume
    - refund volume
    - chargeback volume
    - settlement volume

    """

    payment_daily = (
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
        .groupBy("operation_date")
        .agg(
            countDistinct("payment_id").alias(
                "payment_count"
            ),
            sum(
                coalesce(
                    col("amount"),
                    lit(0),
                )
            ).alias("total_payment_amount"),
            sum(
                when(
                    col("payment_status") == "Captured",
                    col("amount"),
                ).otherwise(0)
            ).alias("captured_payment_amount"),
            sum(
                when(
                    col("payment_status") == "Captured",
                    1,
                ).otherwise(0)
            ).alias("captured_payment_count"),
        )
    )

    refund_daily = (
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
        .groupBy("operation_date")
        .agg(
            countDistinct("refund_id").alias(
                "refund_count"
            ),
            sum(
                coalesce(
                    col("refund_amount"),
                    lit(0),
                )
            ).alias("total_refund_amount"),
        )
    )

    chargeback_daily = (
        chargebacks_df
        .withColumn(
            "operation_date",
            to_date(
                col("created_at")
            ),
        )
        .groupBy("operation_date")
        .agg(
            countDistinct("chargeback_id").alias(
                "chargeback_count"
            ),
            sum(
                coalesce(
                    col("amount"),
                    lit(0),
                )
            ).alias(
                "total_chargeback_amount"
            ),
        )
    )

    settlement_daily = (
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
        .groupBy("operation_date")
        .agg(
            countDistinct("settlement_id").alias(
                "settlement_count"
            ),
            sum(
                coalesce(
                    col("settlement_amount"),
                    lit(0),
                )
            ).alias(
                "total_settlement_amount"
            ),
        )
    )

    return (
        payment_daily
        .join(
            refund_daily,
            on="operation_date",
            how="full",
        )
        .join(
            chargeback_daily,
            on="operation_date",
            how="full",
        )
        .join(
            settlement_daily,
            on="operation_date",
            how="full",
        )
        .select(
            "operation_date",
            coalesce(
                col("payment_count"),
                lit(0),
            ).alias("payment_count"),
            coalesce(
                col("total_payment_amount"),
                lit(0),
            ).alias("total_payment_amount"),
            coalesce(
                col("captured_payment_amount"),
                lit(0),
            ).alias("captured_payment_amount"),
            coalesce(
                col("captured_payment_count"),
                lit(0),
            ).alias("captured_payment_count"),
            coalesce(
                col("refund_count"),
                lit(0),
            ).alias("refund_count"),
            coalesce(
                col("total_refund_amount"),
                lit(0),
            ).alias("total_refund_amount"),
            coalesce(
                col("chargeback_count"),
                lit(0),
            ).alias("chargeback_count"),
            coalesce(
                col("total_chargeback_amount"),
                lit(0),
            ).alias("total_chargeback_amount"),
            coalesce(
                col("settlement_count"),
                lit(0),
            ).alias("settlement_count"),
            coalesce(
                col("total_settlement_amount"),
                lit(0),
            ).alias("total_settlement_amount"),
            (
                coalesce(
                    col("captured_payment_amount"),
                    lit(0),
                )
                - coalesce(
                    col("total_refund_amount"),
                    lit(0),
                )
                - coalesce(
                    col("total_chargeback_amount"),
                    lit(0),
                )
            ).alias("net_payment_amount"),
        )
    )