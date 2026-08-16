"""
batch_generator.py

Creates controlled batches of new events for incremental pipeline testing.

Responsibility:
- Generate genuinely new business events from existing seed events.
- Preserve valid reference IDs.
- Create new primary event IDs.
- Keep SEED mode completely unchanged.
"""

from copy import deepcopy
from uuid import uuid4

import pandas as pd

from topics import TOPICS


# ---------------------------------------------------------------------
# Primary ID fields
# ---------------------------------------------------------------------

PRIMARY_ID_FIELDS = {
    "merchant-events": ["merchant_id"],
    "customer-events": ["customer_id"],
    "invoice-events": ["invoice_id"],
    "payment-events": ["payment_id", "payment_reference"],
    "refund-events": ["refund_id"],
    "chargeback-events": ["chargeback_id"],
    "settlement-events": ["settlement_id"],
}


# ---------------------------------------------------------------------
# Generate Controlled Batch
# ---------------------------------------------------------------------


def build_new_event_batch(
    datasets: dict[str, pd.DataFrame],
    event_count: int,
) -> list[dict[str, object]]:
    """
    Generate a controlled batch of genuinely new events.

    Existing reference IDs are preserved.
    Primary business IDs are regenerated to avoid collisions.

    Parameters
    ----------
    datasets:
        Seed datasets loaded by the existing loader.

    event_count:
        Total number of new events to create.

    Returns
    -------
    list[dict[str, object]]
        New events ready for Kafka publishing.
    """

    if event_count <= 0:
        raise ValueError("NEW_EVENT_COUNT must be greater than zero.")

    source_events: list[dict[str, object]] = []

    for dataset_name, dataframe in datasets.items():
        if dataset_name not in TOPICS:
            continue

        dataframe = dataframe.astype(object).where(
            pd.notna(dataframe),
            None,
        )

        topic = TOPICS[dataset_name]

        for row in dataframe.to_dict(orient="records"):
            source_events.append(
                {
                    "topic": topic,
                    "payload": row,
                }
            )

    if not source_events:
        raise ValueError("No source events available to build the controlled batch.")

    events: list[dict[str, object]] = []

    # -----------------------------------------------------------------
    # Generate requested number of events
    # -----------------------------------------------------------------

    for index in range(event_count):
        source_event = source_events[index % len(source_events)]

        event = deepcopy(source_event)

        topic = str(event["topic"])
        payload = dict(event["payload"])

        unique_suffix = uuid4().hex[:12]

        # -------------------------------------------------------------
        # Regenerate primary business IDs
        # -------------------------------------------------------------

        for field_name in PRIMARY_ID_FIELDS.get(
            topic,
            [],
        ):
            if field_name not in payload:
                continue

            original_value = payload[field_name]

            if original_value is None:
                continue

            payload[field_name] = f"{original_value}-B{unique_suffix}"

        event["payload"] = payload

        events.append(event)

    return events
