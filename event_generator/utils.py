"""
utils.py

Utility functions used by the Event Generator.
"""

import random
import time
from typing import Any

Event = dict[str, Any]


def shuffle_events(events: list[Event]) -> None:
    """
    Randomly shuffle all events in-place.
    """
    random.shuffle(events)


def sleep_between_events(delay: float) -> None:
    """
    Pause between publishing events.

    Parameters
    ----------
    delay : float
        Delay in seconds.
    """
    time.sleep(delay)
