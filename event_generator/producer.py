"""
producer.py

Kafka producer for the Event Generator.

Responsibility:
- Create Kafka producer.
- Publish events.
- Close producer gracefully.

No business logic.
No transformations.
"""

import json
from datetime import UTC, datetime
from time import sleep

from config.settings import (
    KAFKA_CONNECT_RETRIES,
    KAFKA_RETRY_DELAY,
    PRODUCER_CONFIG,
)
from confluent_kafka import KafkaException, Producer
from exceptions import KafkaConnectionError
from logger import logger

# ---------------------------------------------------------------------
# Producer
# ---------------------------------------------------------------------


def create_producer() -> Producer:
    """
    Create and return a Kafka producer.

    Retries if Kafka is unavailable.
    """

    for attempt in range(1, KAFKA_CONNECT_RETRIES + 1):
        try:
            producer = Producer(PRODUCER_CONFIG)

            # Force a connection attempt
            producer.list_topics(timeout=5)

            logger.info("Connected to Kafka.")

            return producer

        except KafkaException as error:
            logger.warning(
                "Kafka connection failed (attempt %s/%s): %s",
                attempt,
                KAFKA_CONNECT_RETRIES,
                error,
            )

            if attempt < KAFKA_CONNECT_RETRIES:
                logger.info(
                    "Retrying in %s seconds...",
                    KAFKA_RETRY_DELAY,
                )
                sleep(KAFKA_RETRY_DELAY)

    raise KafkaConnectionError(
        f"Unable to connect to Kafka after {KAFKA_CONNECT_RETRIES} attempts."
    )


# ---------------------------------------------------------------------
# Delivery Callback
# ---------------------------------------------------------------------


def delivery_report(err, msg) -> None:
    """
    Delivery callback.

    Called once Kafka acknowledges the message.
    """

    if err is not None:
        logger.error("Failed to deliver message: %s", err)


# ---------------------------------------------------------------------
# Publish Event
# ---------------------------------------------------------------------


def publish_event(
    producer: Producer,
    event: dict[str, object],
) -> None:
    """
    Publish a single event to Kafka.
    """

    message = {
        "timestamp": datetime.now(UTC).isoformat(),
        "payload": event["payload"],
    }

    producer.produce(
        topic=str(event["topic"]),
        value=json.dumps(message).encode("utf-8"),
        callback=delivery_report,
    )

    # Trigger delivery callbacks
    producer.poll(0)


# ---------------------------------------------------------------------
# Close Producer
# ---------------------------------------------------------------------


def close_producer(producer: Producer) -> None:
    """
    Flush pending messages before exiting.
    """

    producer.flush()
