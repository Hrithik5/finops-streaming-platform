"""
settings.py

Centralized configuration for the Event Generator.
"""

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


# ---------------------------------------------------------------------
# Project Paths
# ---------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

DATA_DIR = PROJECT_ROOT / "data"
SEED_PATH = DATA_DIR / "seed"

# ---------------------------------------------------------------------
# Batch Event Generator
# ---------------------------------------------------------------------

EVENT_MODE = "NEW_BATCH"

NEW_EVENT_COUNT = 25

# ---------------------------------------------------------------------
# Event Generator
# ---------------------------------------------------------------------

EVENT_DELAY = 0.01


# ---------------------------------------------------------------------
# Kafka Connection
# ---------------------------------------------------------------------

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_API_KEY = os.getenv("KAFKA_API_KEY")
KAFKA_API_SECRET = os.getenv("KAFKA_API_SECRET")


# ---------------------------------------------------------------------
# Retry Configuration
# ---------------------------------------------------------------------

KAFKA_CONNECT_RETRIES = 3
KAFKA_RETRY_DELAY = 2


# ---------------------------------------------------------------------
# Kafka Producer Configuration
# ---------------------------------------------------------------------

PRODUCER_CONFIG = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "PLAIN",
    "sasl.username": KAFKA_API_KEY,
    "sasl.password": KAFKA_API_SECRET,
    # Reliability
    "enable.idempotence": True,
    "acks": "all",
    "retries": 3,
}
