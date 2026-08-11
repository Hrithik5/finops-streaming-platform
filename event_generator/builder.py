"""
builder.py

Builds event objects from the loaded seed datasets.

Responsibility:
- Convert every CSV row into an event.
- Attach the appropriate Kafka topic.

No joins.
No transformations.
No business logic.
"""

from collections.abc import Mapping

import pandas as pd
from topics import TOPICS


def build_events(
    datasets: Mapping[str, pd.DataFrame],
) -> list[dict[str, object]]:
    """
    Convert all datasets into Kafka events.

    Parameters
    ----------
    datasets : Mapping[str, pd.DataFrame]
        Dictionary containing all loaded Pandas DataFrames.

    Returns
    -------
    list[dict[str, object]]
        List of event objects ready to be published.
    """

    events: list[dict[str, object]] = []

    for dataset_name, dataframe in datasets.items():
        # Skip lookup tables
        if dataset_name not in TOPICS:
            continue

        topic = TOPICS[dataset_name]

        # Convert NaN values to None (JSON null)
        dataframe = dataframe.astype(object).where(pd.notna(dataframe), None)

        for row in dataframe.to_dict(orient="records"):
            events.append(
                {
                    "topic": topic,
                    "payload": row,
                }
            )

    return events
