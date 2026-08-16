"""
03_gold_processing.py

Incremental Gold processing.

Current scope:
    payment_performance only.

Flow:
    Bronze incremental stream
          ↓
    Identify affected payment_ids
          ↓
    Read current Silver state
          ↓
    Rebuild affected payment metrics
          ↓
    MERGE into Gold
          ↓
    Gold checkpoint
"""

from streaming.gold.incremental_metrics import (
    build_incremental_payment_performance,
)
from streaming.gold.metrics import (
    build_payment_performance,
)
from streaming.gold.writer import (
    write_gold_incremental,
)


# =====================================================================
# Read Silver Tables
# =====================================================================

payments_df = spark.read.table("dev.silver.payments")

merchants_df = spark.read.table("dev.silver.merchants")

gateways_df = spark.read.table("dev.silver.payment_gateways")

payment_methods_df = spark.read.table("dev.silver.payment_methods")

attempts_df = spark.read.table("dev.silver.payment_attempts")

events_df = spark.read.table("dev.silver.payment_events")

refunds_df = spark.read.table("dev.silver.refunds")

chargebacks_df = spark.read.table("dev.silver.chargebacks")

settlements_df = spark.read.table("dev.silver.settlements")


# =====================================================================
# Read New Bronze Records
# =====================================================================

bronze_stream_df = spark.readStream.table("dev.bronze.raw_events")


# =====================================================================
# Process Incremental Gold Batch
# =====================================================================


def process_gold_batch(
    batch_df,
    batch_id: int,
) -> None:
    """
    Process one incremental Bronze batch.

    Only payment-related records are used to identify
    affected payment_ids.
    """

    if batch_df.isEmpty():
        return

    payment_related_topics = [
        "payment-events",
        "refund-events",
        "chargeback-events",
        "settlement-events",
    ]

    payment_batch = batch_df.filter(batch_df.topic.isin(payment_related_topics))

    if payment_batch.isEmpty():
        return

    payment_ids_df = payment_batch.select(
        "raw_payload",
        "topic",
    )

    # -------------------------------------------------------------
    # Parse payment IDs from raw Bronze payload
    # -------------------------------------------------------------

    from pyspark.sql.functions import (
        col,
        from_json,
    )

    from streaming.silver.schemas import (
        EVENT_SCHEMAS,
    )

    parsed_ids = []

    for topic in payment_related_topics:
        topic_df = payment_batch.filter(col("topic") == topic)

        if topic_df.isEmpty():
            continue

        parsed_topic = topic_df.select(
            from_json(
                col("raw_payload"),
                EVENT_SCHEMAS[topic],
            ).alias("event")
        )

        parsed_topic = parsed_topic.select("event.payload.*").select("payment_id")

        parsed_ids.append(parsed_topic)

    if not parsed_ids:
        return

    affected_payment_ids = parsed_ids[0]

    for additional_df in parsed_ids[1:]:
        affected_payment_ids = affected_payment_ids.unionByName(additional_df)

    affected_payment_ids = affected_payment_ids.where(
        col("payment_id").isNotNull()
    ).distinct()

    if affected_payment_ids.isEmpty():
        return

    # -------------------------------------------------------------
    # Rebuild affected Gold records
    # -------------------------------------------------------------

    payment_performance_df = build_incremental_payment_performance(
        payment_ids_df=affected_payment_ids,
        payments_df=payments_df,
        merchants_df=merchants_df,
        gateways_df=gateways_df,
        payment_methods_df=payment_methods_df,
        attempts_df=attempts_df,
        events_df=events_df,
        refunds_df=refunds_df,
        chargebacks_df=chargebacks_df,
        settlements_df=settlements_df,
        build_payment_performance_func=(build_payment_performance),
    )

    # -------------------------------------------------------------
    # Write affected Gold rows
    # -------------------------------------------------------------

    write_gold_incremental(
        payment_performance_df,
        "dev.gold.payment_performance",
        ["payment_id"],
    )

    print(f"Gold batch {batch_id} completed.")


# =====================================================================
# Start Incremental Gold Query
# =====================================================================

gold_query = (
    bronze_stream_df.writeStream.foreachBatch(process_gold_batch)
    .option(
        "checkpointLocation",
        "/Volumes/dev/stream/streaming_checkpoints/gold_payment",
    )
    .trigger(availableNow=True)
    .start()
)


# =====================================================================
# Wait
# =====================================================================

gold_query.awaitTermination()

print("Incremental payment Gold processing completed.")

