"""
logger.py

Centralized logging configuration for the project.
"""

import logging
import sys

# ---------------------------------------------------------------------
# Logger Configuration
# ---------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
    ],
)

logger = logging.getLogger("event-generator")
