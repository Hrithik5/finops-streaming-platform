"""
01_static_ingestion.py

Static seed data → Bronze ingestion.
"""

from streaming.bronze.static_ingestion import (
    read_static_csv,
    write_static_bronze,
)


# ---------------------------------------------------------------------
# Static Datasets
# ---------------------------------------------------------------------

STATIC_DATASETS = {
    "merchant_bank_accounts": (
        "/Volumes/dev/stream/static_seed/"
        "merchant_bank_accounts_dataset.csv"
    ),
    "payment_methods": (
        "/Volumes/dev/stream/static_seed/"
        "payment_methods_dataset.csv"
    ),
    "payment_gateways": (
        "/Volumes/dev/stream/static_seed/"
        "payment_gateways_dataset.csv"
    ),
    "payment_attempts": (
        "/Volumes/dev/stream/static_seed/"
        "payment_attempts_dataset.csv"
    ),
    "payment_events": (
        "/Volumes/dev/stream/static_seed/"
        "payment_events_dataset.csv"
    ),
}


# ---------------------------------------------------------------------
# Process Each Dataset
# ---------------------------------------------------------------------

for dataset_name, path in STATIC_DATASETS.items():

    df = read_static_csv(
        spark,
        path,
    )

    bronze_table = (
        f"dev.bronze.{dataset_name}"
    )

    write_static_bronze(
        df,
        bronze_table,
    )


print("Static Bronze ingestion completed successfully.")