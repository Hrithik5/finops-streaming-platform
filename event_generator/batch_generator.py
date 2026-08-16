"""
batch_generator.py

Creates controlled, correlated event lifecycles.

One lifecycle creates:
    merchant
    customer
    invoice
    payment
    refund
    chargeback
    settlement

All IDs and foreign-key relationships are internally consistent.

Responsibility:
- Generate genuinely new business entities/events.
- Preserve the existing event schemas.
- Preserve valid cross-entity relationships.
- Generate unique IDs for every lifecycle.
"""

from copy import deepcopy
from datetime import datetime, timezone
from uuid import uuid4

import pandas as pd

from topics import TOPICS


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------


def _now() -> str:
    """Return current UTC timestamp in ISO format."""

    return datetime.now(timezone.utc).isoformat()


def _unique_id(prefix: str) -> str:
    """Generate a unique business ID."""

    return f"{prefix}-batch-{uuid4().hex[:12]}"


def _prepare_dataframe(
    dataframe: pd.DataFrame,
) -> list[dict[str, object]]:
    """Convert a DataFrame into clean Python records."""

    dataframe = dataframe.astype(object).where(
        pd.notna(dataframe),
        None,
    )

    return dataframe.to_dict(orient="records")


def _template_row(
    datasets: dict[str, pd.DataFrame],
    dataset_name: str,
) -> dict[str, object]:

    if dataset_name not in datasets:
        raise ValueError(f"Required dataset not found: {dataset_name}")

    rows = _prepare_dataframe(datasets[dataset_name])

    if not rows:
        raise ValueError(f"Dataset is empty: {dataset_name}")

    return deepcopy(rows[0])


# ---------------------------------------------------------------------
# Lifecycle Generator
# ---------------------------------------------------------------------


def build_new_event_batch(
    datasets: dict[str, pd.DataFrame],
    lifecycle_count: int,
) -> list[dict[str, object]]:
    """
    Generate complete correlated business lifecycles.

    One lifecycle produces seven Kafka events:

        merchant
        customer
        invoice
        payment
        refund
        chargeback
        settlement

    Foreign keys are aligned across all events.
    """

    if lifecycle_count <= 0:
        raise ValueError("NEW_LIFECYCLE_COUNT must be greater than zero.")

    required_datasets = [
        "merchants",
        "customers",
        "invoices",
        "payments",
        "refunds",
        "chargebacks",
        "settlements",
    ]

    for dataset_name in required_datasets:
        if dataset_name not in datasets:
            raise ValueError(
                f"Missing dataset required for "
                f"controlled lifecycle generation: "
                f"{dataset_name}"
            )

    events: list[dict[str, object]] = []

    # -----------------------------------------------------------------
    # Generate each lifecycle
    # -----------------------------------------------------------------

    for _ in range(lifecycle_count):
        # -------------------------------------------------------------
        # Generate core IDs
        # -------------------------------------------------------------

        merchant_id = _unique_id("merchant")

        customer_id = _unique_id("customer")

        invoice_id = _unique_id("invoice")

        payment_id = _unique_id("payment")

        payment_reference = _unique_id("payment-ref")

        refund_id = _unique_id("refund")

        chargeback_id = _unique_id("chargeback")

        settlement_id = _unique_id("settlement")

        account_id = _unique_id("account")

        # -------------------------------------------------------------
        # Common timestamp
        # -------------------------------------------------------------

        timestamp = _now()

        # =============================================================
        # Merchant
        # =============================================================

        merchant = _template_row(
            datasets,
            "merchants",
        )

        merchant["merchant_id"] = merchant_id

        if "created_at" in merchant:
            merchant["created_at"] = timestamp

        if "updated_at" in merchant:
            merchant["updated_at"] = timestamp

        events.append(
            {
                "topic": TOPICS["merchants"],
                "payload": merchant,
            }
        )

        # =============================================================
        # Customer
        # =============================================================

        customer = _template_row(
            datasets,
            "customers",
        )

        customer["customer_id"] = customer_id

        if "created_at" in customer:
            customer["created_at"] = timestamp

        events.append(
            {
                "topic": TOPICS["customers"],
                "payload": customer,
            }
        )

        # =============================================================
        # Invoice
        # =============================================================

        invoice = _template_row(
            datasets,
            "invoices",
        )

        invoice["invoice_id"] = invoice_id
        invoice["customer_id"] = customer_id
        invoice["merchant_id"] = merchant_id

        if "created_at" in invoice:
            invoice["created_at"] = timestamp

        if "invoice_date" in invoice:
            invoice["invoice_date"] = timestamp

        events.append(
            {
                "topic": TOPICS["invoices"],
                "payload": invoice,
            }
        )

        # =============================================================
        # Payment
        # =============================================================

        payment = _template_row(
            datasets,
            "payments",
        )

        payment["payment_id"] = payment_id
        payment["invoice_id"] = invoice_id
        payment["merchant_id"] = merchant_id
        payment["customer_id"] = customer_id
        payment["payment_reference"] = payment_reference

        if "payment_status" in payment:
            payment["payment_status"] = "Captured"

        if "initiated_at" in payment:
            payment["initiated_at"] = timestamp

        if "created_at" in payment:
            payment["created_at"] = timestamp

        # Keep payment amount from the seed template.
        payment_amount = payment.get(
            "amount",
            0,
        )

        if payment_amount is None:
            payment_amount = 0

        payment_amount = int(payment_amount)

        # =============================================================
        # Refund
        # =============================================================

        refund = _template_row(
            datasets,
            "refunds",
        )

        refund["refund_id"] = refund_id
        refund["payment_id"] = payment_id
        refund["merchant_id"] = merchant_id

        refund_amount = max(
            1,
            min(
                payment_amount,
                int(payment_amount * 0.20),
            ),
        )

        refund["refund_amount"] = refund_amount

        if "requested_at" in refund:
            refund["requested_at"] = timestamp

        if "completed_at" in refund:
            refund["completed_at"] = timestamp

        # =============================================================
        # Chargeback
        # =============================================================

        chargeback = _template_row(
            datasets,
            "chargebacks",
        )

        chargeback["chargeback_id"] = chargeback_id

        chargeback["payment_id"] = payment_id
        chargeback["merchant_id"] = merchant_id

        chargeback_amount = max(
            1,
            min(
                payment_amount - refund_amount,
                int(payment_amount * 0.30),
            ),
        )

        chargeback["amount"] = chargeback_amount

        if "created_at" in chargeback:
            chargeback["created_at"] = timestamp

        if "resolved_at" in chargeback:
            chargeback["resolved_at"] = timestamp

        # =============================================================
        # Settlement
        # =============================================================

        settlement = _template_row(
            datasets,
            "settlements",
        )

        settlement["settlement_id"] = settlement_id

        settlement["merchant_id"] = merchant_id

        settlement["payment_id"] = payment_id

        settlement["account_id"] = account_id

        settlement_amount = max(
            0,
            payment_amount - refund_amount - chargeback_amount,
        )

        settlement["settlement_amount"] = settlement_amount

        if "initiated_at" in settlement:
            settlement["initiated_at"] = timestamp

        if "completed_at" in settlement:
            settlement["completed_at"] = timestamp

        # =============================================================
        # Append financial events
        # =============================================================

        events.extend(
            [
                {
                    "topic": TOPICS["payments"],
                    "payload": payment,
                },
                {
                    "topic": TOPICS["refunds"],
                    "payload": refund,
                },
                {
                    "topic": TOPICS["chargebacks"],
                    "payload": chargeback,
                },
                {
                    "topic": TOPICS["settlements"],
                    "payload": settlement,
                },
            ]
        )

    return events
