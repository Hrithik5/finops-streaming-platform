"""
main.py

Event Generator.

Modes:
- SEED      : Publish the original seed events.
- NEW_BATCH : Publish a controlled batch of genuinely new events.
"""

from collections import Counter
from time import perf_counter

from batch_generator import build_new_event_batch
from builder import build_events
from config.settings import (
    EVENT_DELAY,
    EVENT_MODE,
    NEW_EVENT_COUNT,
    SEED_PATH,
)
from exceptions import KafkaConnectionError
from loader import load_datasets
from logger import logger
from producer import (
    close_producer,
    create_producer,
    publish_event,
)
from utils import shuffle_events, sleep_between_events


# ---------------------------------------------------------------------
# Build Events
# ---------------------------------------------------------------------


def build_event_batch(datasets):
    """
    Build events according to the configured generator mode.
    """

    if EVENT_MODE == "SEED":
        return build_events(datasets)

    if EVENT_MODE == "NEW_BATCH":
        return build_new_event_batch(
            datasets,
            NEW_EVENT_COUNT,
        )

    raise ValueError(f"Unsupported EVENT_MODE: {EVENT_MODE}")


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------


def main() -> None:
    """Run the Event Generator."""

    logger.info(
        "Starting Event Generator in %s mode.",
        EVENT_MODE,
    )

    datasets = load_datasets(SEED_PATH)

    logger.info(
        "Loaded %s datasets.",
        len(datasets),
    )

    events = build_event_batch(datasets)

    logger.info(
        "Generated %s events.",
        len(events),
    )

    # -----------------------------------------------------------------
    # Event Summary
    # -----------------------------------------------------------------

    topic_counts = Counter(event["topic"] for event in events)

    print()
    print("=" * 60)
    print("Event Summary")
    print("=" * 60)

    for topic, count in sorted(topic_counts.items()):
        print(f"{topic:<25} : {count}")

    print(f"\nTotal Events: {len(events)}")

    print(f"Mode        : {EVENT_MODE}")

    print("=" * 60)
    print()

    # -----------------------------------------------------------------
    # Shuffle
    # -----------------------------------------------------------------

    logger.info("Shuffling events...")

    shuffle_events(events)

    logger.info("Events shuffled successfully.")

    # -----------------------------------------------------------------
    # Kafka Producer
    # -----------------------------------------------------------------

    producer = None

    try:
        logger.info("Connecting to Kafka...")

        producer = create_producer()

        logger.info("Kafka producer created successfully.")

        # -------------------------------------------------------------
        # Publish
        # -------------------------------------------------------------

        logger.info("Publishing events...")

        start_time = perf_counter()

        for index, event in enumerate(
            events,
            start=1,
        ):
            publish_event(
                producer,
                event,
            )

            sleep_between_events(EVENT_DELAY)

            if index % 100 == 0:
                logger.info(
                    "Published %s/%s events.",
                    index,
                    len(events),
                )

        end_time = perf_counter()

        # -------------------------------------------------------------
        # Statistics
        # -------------------------------------------------------------

        duration = end_time - start_time

        events_per_second = len(events) / duration if duration > 0 else 0

        print()
        print("=" * 60)
        print("Publishing Statistics")
        print("=" * 60)
        print(f"Total Events      : {len(events)}")
        print(f"Publishing Time   : {duration:.2f} seconds")
        print(f"Throughput        : {events_per_second:.2f} events/sec")
        print("=" * 60)
        print()

        logger.info("Event Generator completed successfully.")

    except KafkaConnectionError as error:
        logger.error(
            "%s",
            error,
        )

    finally:
        if producer is not None:
            close_producer(producer)

            logger.info("Kafka producer closed.")


if __name__ == "__main__":
    main()
