"""
loader.py

Loads all seed CSV datasets into memory.

Responsibility:
- Read all CSV files from the seed directory.
- Return them as a dictionary of Pandas DataFrames.

No transformations.
No validation.
No joins.
"""

from pathlib import Path

import pandas as pd
from datasets import DATASETS

# ---------------------------------------------------------------------
# Load Seed Data
# ---------------------------------------------------------------------


def load_datasets(seed_path: Path) -> dict[str, pd.DataFrame]:
    """
    Load all seed datasets.

    Parameters
    ----------
    seed_path : Path
        Path to the seed data directory.

    Returns
    -------
    dict[str, pd.DataFrame]
        Dictionary containing all datasets as Pandas DataFrames.
    """

    datasets: dict[str, pd.DataFrame] = {}

    for name, filename in DATASETS.items():
        file_path = seed_path / filename
        datasets[name] = pd.read_csv(file_path)

    return datasets
