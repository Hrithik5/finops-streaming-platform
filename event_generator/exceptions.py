"""
exceptions.py

Custom exceptions used throughout the Event Generator.
"""


class KafkaConnectionError(Exception):
    """
    Raised when the application is unable to connect
    to the Kafka broker.
    """

