"""
batch_generator.py

Controlled business-event generator.

Design goals:
- Generate genuinely new IDs with no semantic prefixes.
- Use realistic/randomized amounts.
- Use realistic/randomized timestamps.
- Randomize payment status.
- Randomly select existing merchants, bank accounts,
  gateways, and payment methods.
- Preserve foreign-key relationships.
- Generate dependent financial events only when appropriate.
"""

from copy import deepcopy
from datetime import datetime, timedelta, timezone
from random import choice, randint, uniform
from uuid import uuid4

import pandas as pd

from topics import TOPICS


# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------

MIN_PAYMENT_AMOUNT = 500
MAX_PAYMENT_AMOUNT = 15000

MIN_REFUND_PERCENT = 0.05
MAX_REFUND_PERCENT = 0.40

MIN_CHARGEBACK_PERCENT = 0.05
MAX_CHARGEBACK_PERCENT = 0.30

MAX_EVENT_AGE_DAYS = 30


PAYMENT_STATUSES = [
    "Captured",
    "Failed",
    "Pending",
]


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------


def _now() -> datetime:
    """Return current UTC datetime."""

    return datetime.now(timezone.utc)


def _unique_id() -> str:
    """Generate a plain ID without any semantic prefix."""

    return uuid4().hex[:12]


def _random_amount() -> int:
    """Generate a realistic payment amount."""

    return randint(
        MIN_PAYMENT_AMOUNT,
        MAX_PAYMENT_AMOUNT,
    )


def _random_timestamp(
    base: datetime | None = None,
    min_minutes: int = 0,
    max_minutes: int = 0,
) -> str:
    """
    Generate a timestamp.

    If base is supplied, add a random number of minutes.
    Otherwise generate a timestamp somewhere in the last
    MAX_EVENT_AGE_DAYS.
    """

    if base is None:
        base = _now() - timedelta(
            days=randint(
                0,
                MAX_EVENT_AGE_DAYS,
            ),
            hours=randint(0, 23),
            minutes=randint(0, 59),
        )

    if max_minutes > 0:
        base = base + timedelta(
            minutes=randint(
                min_minutes,
                max_minutes,
            )
        )

    return base.isoformat()


def _prepare_dataframe(
    dataframe: pd.DataFrame,
) -> list[dict[str, object]]:
    """Convert a DataFrame into clean Python records."""

    dataframe = dataframe.astype(object).where(
        pd.notna(dataframe),
        None,
    )

    return dataframe.to_dict(orient="records")


def _random_template(
    datasets: dict[str, pd.DataFrame],
    dataset_name: str,
) -> dict[str, object]:
    """
    Return a random source row from a dataset.

    This prevents every generated record from inheriting
    exactly the same template values.
    """

    if dataset_name not in datasets:
        raise ValueError(f"Required dataset not found: {dataset_name}")

    rows = _prepare_dataframe(datasets[dataset_name])

    if not rows:
        raise ValueError(f"Dataset is empty: {dataset_name}")

    return deepcopy(choice(rows))


# ---------------------------------------------------------------------
# Merchant / Account Pool
# ---------------------------------------------------------------------


def _build_merchant_account_pool(
    datasets: dict[str, pd.DataFrame],
) -> list[
    tuple[
        dict[str, object],
        dict[str, object],
    ]
]:
    """
    Build valid merchant/account combinations.

    Every selected account belongs to the selected merchant.
    """

    merchants = _prepare_dataframe(datasets["merchants"])

    accounts = _prepare_dataframe(datasets["merchant_bank_accounts"])

    accounts_by_merchant: dict[object, list[dict[str, object]]] = {}

    for account in accounts:
        merchant_id = account.get("merchant_id")

        accounts_by_merchant.setdefault(
            merchant_id,
            [],
        ).append(account)

    pool = []

    for merchant in merchants:
        merchant_id = merchant.get("merchant_id")

        merchant_accounts = accounts_by_merchant.get(
            merchant_id,
            [],
        )

        if not merchant_accounts:
            continue

        pool.append(
            (
                deepcopy(merchant),
                deepcopy(choice(merchant_accounts)),
            )
        )

    if not pool:
        raise ValueError("No valid merchant/account combinations found.")

    return pool


# ---------------------------------------------------------------------
# Event Generator
# ---------------------------------------------------------------------


def build_new_event_batch(
    datasets: dict[str, pd.DataFrame],
    lifecycle_count: int,
) -> list[dict[str, object]]:
    """
    Generate a controlled batch of realistic business events.

    A lifecycle always contains:
        merchant
        customer
        invoice
        payment

    A captured payment additionally produces:
        refund
        chargeback
        settlement

    Therefore the final event count is variable.
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
        "merchant_bank_accounts",
    ]

    for dataset_name in required_datasets:
        if dataset_name not in datasets:
            raise ValueError(f"Missing required dataset: {dataset_name}")

    merchant_account_pool = _build_merchant_account_pool(datasets)

    events: list[dict[str, object]] = []

    # -----------------------------------------------------------------
    # Generate lifecycles
    # -----------------------------------------------------------------

    for _ in range(lifecycle_count):
        # -------------------------------------------------------------
        # Existing valid merchant + account
        # -------------------------------------------------------------

        merchant, account = choice(merchant_account_pool)

        merchant_id = merchant["merchant_id"]

        account_id = account["account_id"]

        # -------------------------------------------------------------
        # New business IDs
        # -------------------------------------------------------------

        customer_id = _unique_id()
        invoice_id = _unique_id()
        payment_id = _unique_id()
        payment_reference = _unique_id()
        refund_id = _unique_id()
        chargeback_id = _unique_id()
        settlement_id = _unique_id()

        # -------------------------------------------------------------
        # Payment characteristics
        # -------------------------------------------------------------

        payment_status = choice(PAYMENT_STATUSES)

        payment_amount = _random_amount()

        # -------------------------------------------------------------
        # Timeline
        # -------------------------------------------------------------

        initiated_at = _now() - timedelta(
            days=randint(
                0,
                MAX_EVENT_AGE_DAYS,
            ),
            hours=randint(
                0,
                23,
            ),
            minutes=randint(
                0,
                59,
            ),
        )

        completed_at = None

        if payment_status == "Captured":
            completed_at = initiated_at + timedelta(
                minutes=randint(
                    1,
                    30,
                )
            )

        # =============================================================
        # Merchant
        # =============================================================

        merchant_event = deepcopy(merchant)

        timestamp = _random_timestamp()

        if "created_at" in merchant_event:
            merchant_event["created_at"] = timestamp

        if "updated_at" in merchant_event:
            merchant_event["updated_at"] = timestamp

        events.append(
            {
                "topic": TOPICS["merchants"],
                "payload": merchant_event,
            }
        )

        # =============================================================
        # Customer
        # =============================================================

        customer = _random_template(
            datasets,
            "customers",
        )

        customer["customer_id"] = customer_id

        if "created_at" in customer:
            customer["created_at"] = _random_timestamp(initiated_at)

        events.append(
            {
                "topic": TOPICS["customers"],
                "payload": customer,
            }
        )

        # =============================================================
        # Invoice
        # =============================================================

        invoice = _random_template(
            datasets,
            "invoices",
        )

        invoice["invoice_id"] = invoice_id

        invoice["customer_id"] = customer_id

        invoice["merchant_id"] = merchant_id

        if "invoice_amount" in invoice:
            invoice["invoice_amount"] = payment_amount

        if "amount" in invoice:
            invoice["amount"] = payment_amount

        invoice_date = _random_timestamp(
            initiated_at,
            0,
            120,
        )

        if "invoice_date" in invoice:
            invoice["invoice_date"] = invoice_date

        if "created_at" in invoice:
            invoice["created_at"] = invoice_date

        events.append(
            {
                "topic": TOPICS["invoices"],
                "payload": invoice,
            }
        )

        # =============================================================
        # Payment
        # =============================================================

        payment = _random_template(
            datasets,
            "payments",
        )

        payment["payment_id"] = payment_id

        payment["invoice_id"] = invoice_id

        payment["merchant_id"] = merchant_id

        payment["customer_id"] = customer_id

        payment["payment_reference"] = payment_reference

        payment["amount"] = payment_amount

        payment["payment_status"] = payment_status

        if "initiated_at" in payment:
            payment["initiated_at"] = initiated_at.isoformat()

        if "completed_at" in payment:
            payment["completed_at"] = completed_at.isoformat() if completed_at else None

        events.append(
            {
                "topic": TOPICS["payments"],
                "payload": payment,
            }
        )

        # =============================================================
        # Dependent financial events
        # =============================================================

        if payment_status != "Captured":
            continue

        # -------------------------------------------------------------
        # Refund
        # -------------------------------------------------------------

        refund = _random_template(
            datasets,
            "refunds",
        )

        refund["refund_id"] = refund_id

        refund["payment_id"] = payment_id

        refund["merchant_id"] = merchant_id

        refund_amount = int(
            payment_amount
            * uniform(
                MIN_REFUND_PERCENT,
                MAX_REFUND_PERCENT,
            )
        )

        refund["refund_amount"] = min(
            payment_amount,
            max(
                1,
                refund_amount,
            ),
        )

        refund_time = _random_timestamp(
            completed_at,
            60,
            60 * 72,
        )

        if "requested_at" in refund:
            refund["requested_at"] = refund_time

        if "completed_at" in refund:
            refund["completed_at"] = _random_timestamp(
                completed_at,
                120,
                60 * 96,
            )

        events.append(
            {
                "topic": TOPICS["refunds"],
                "payload": refund,
            }
        )

        # -------------------------------------------------------------
        # Chargeback
        # -------------------------------------------------------------

        chargeback = _random_template(
            datasets,
            "chargebacks",
        )

        chargeback["chargeback_id"] = chargeback_id

        chargeback["payment_id"] = payment_id

        chargeback["merchant_id"] = merchant_id

        remaining_amount = max(
            0,
            payment_amount - refund["refund_amount"],
        )

        chargeback_amount = int(
            remaining_amount
            * uniform(
                MIN_CHARGEBACK_PERCENT,
                MAX_CHARGEBACK_PERCENT,
            )
        )

        chargeback["amount"] = max(
            0,
            chargeback_amount,
        )

        chargeback_time = _random_timestamp(
            completed_at,
            60 * 24,
            60 * 24 * 10,
        )

        if "created_at" in chargeback:
            chargeback["created_at"] = chargeback_time

        if "resolved_at" in chargeback:
            chargeback["resolved_at"] = _random_timestamp(
                datetime.fromisoformat(chargeback_time),
                60 * 24,
                60 * 24 * 14,
            )

        events.append(
            {
                "topic": TOPICS["chargebacks"],
                "payload": chargeback,
            }
        )

        # -------------------------------------------------------------
        # Settlement
        # -------------------------------------------------------------

        settlement = _random_template(
            datasets,
            "settlements",
        )

        settlement["settlement_id"] = settlement_id

        settlement["payment_id"] = payment_id

        settlement["merchant_id"] = merchant_id

        settlement["account_id"] = account_id

        settlement_amount = max(
            0,
            payment_amount - refund["refund_amount"] - chargeback["amount"],
        )

        settlement["settlement_amount"] = settlement_amount

        settlement_time = _random_timestamp(
            completed_at,
            60 * 24,
            60 * 24 * 5,
        )

        if "initiated_at" in settlement:
            settlement["initiated_at"] = settlement_time

        if "completed_at" in settlement:
            settlement["completed_at"] = _random_timestamp(
                datetime.fromisoformat(settlement_time),
                60,
                60 * 48,
            )

        events.append(
            {
                "topic": TOPICS["settlements"],
                "payload": settlement,
            }
        )

    return events
