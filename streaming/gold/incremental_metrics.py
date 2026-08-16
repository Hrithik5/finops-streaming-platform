"""
Incremental Gold metrics.

Responsibility:
- Rebuild payment-level Gold records only for affected payment_ids.
- Read the current Silver state for those payments.
- Preserve the existing full-refresh Gold logic.

No writing.
No orchestration.
"""

from pyspark.sql import DataFrame


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
    """
    Rebuild payment_performance only for affected payment_ids.

    payment_ids_df must contain:
        payment_id
    """

    affected_payment_ids = (
        payment_ids_df.select("payment_id").where("payment_id IS NOT NULL").distinct()
    )

    affected_payments = payments_df.join(
        affected_payment_ids,
        on="payment_id",
        how="inner",
    )

    affected_attempts = attempts_df.join(
        affected_payment_ids,
        on="payment_id",
        how="inner",
    )

    affected_events = events_df.join(
        affected_payment_ids,
        on="payment_id",
        how="inner",
    )

    affected_refunds = refunds_df.join(
        affected_payment_ids,
        on="payment_id",
        how="inner",
    )

    affected_chargebacks = chargebacks_df.join(
        affected_payment_ids,
        on="payment_id",
        how="inner",
    )

    affected_settlements = settlements_df.join(
        affected_payment_ids,
        on="payment_id",
        how="inner",
    )

    return build_payment_performance_func(
        payments_df=affected_payments,
        merchants_df=merchants_df,
        gateways_df=gateways_df,
        payment_methods_df=payment_methods_df,
        attempts_df=affected_attempts,
        events_df=affected_events,
        refunds_df=affected_refunds,
        chargebacks_df=affected_chargebacks,
        settlements_df=affected_settlements,
    )
