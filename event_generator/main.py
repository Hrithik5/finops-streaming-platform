"""
main.py

Event Generator

Pipeline:
1. Load seed datasets.
2. Build events.
3. Shuffle events.
4. Publish events to Kafka.
"""

from collections import Counter
from time import perf_counter

from builder import build_events
from config.settings import (
    EVENT_DELAY,
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
# Main
# ---------------------------------------------------------------------


def main() -> None:
    """Run the Event Generator."""

    logger.info("Loading seed datasets...")

    datasets = load_datasets(SEED_PATH)

    logger.info("Loaded %s datasets.", len(datasets))

    logger.info("Building events...")

    events = build_events(datasets)

    logger.info("Generated %s events.", len(events))

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
    print("=" * 60)
    print()

    # -----------------------------------------------------------------
    # Shuffle Events
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
        # Publish Events
        # -------------------------------------------------------------

        logger.info("Publishing events...")

        start_time = perf_counter()

        for index, event in enumerate(events, start=1):
            publish_event(producer, event)
            sleep_between_events(EVENT_DELAY)

            if index % 100 == 0:
                logger.info(
                    "Published %s/%s events.",
                    index,
                    len(events),
                )

        end_time = perf_counter()

        # -------------------------------------------------------------
        # Publishing Statistics
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
        logger.error("%s", error)

    finally:
        if producer is not None:
            close_producer(producer)
            logger.info("Kafka producer closed.")


if __name__ == "__main__":
    main()
