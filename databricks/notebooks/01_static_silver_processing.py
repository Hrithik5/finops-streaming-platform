"""
01_static_silver_processing.py

Static Bronze → Silver pipeline.
"""

from streaming.silver.static_parser import read_static_bronze
from streaming.silver.static_transformer import transform_static
from streaming.silver.static_quality import count_duplicates
from streaming.silver.static_writer import write_static_silver


DATASETS = {
    "merchant_bank_accounts": {
        "key_columns": ["account_id"],
    },
    "payment_methods": {
        "key_columns": ["payment_method_id"],
    },
    "payment_gateways": {
        "key_columns": ["gateway_id"],
    },
    "payment_attempts": {
        "key_columns": ["attempt_id"],
    },
    "payment_events": {
        "key_columns": ["event_id"],
    },
}


for dataset_name, config in DATASETS.items():

    bronze_table = f"dev.bronze.{dataset_name}"
    silver_table = f"dev.silver.{dataset_name}"

    df = read_static_bronze(
        spark,
        bronze_table,
    )

    transformed_df = transform_static(df)

    duplicates = count_duplicates(
        transformed_df,
        config["key_columns"],
    )

    print(f"\n{'=' * 60}")
    print(dataset_name)
    print("=" * 60)
    print(f"Rows       : {transformed_df.count()}")
    print(f"Duplicates : {duplicates}")

    write_static_silver(
        transformed_df,
        silver_table,
    )

    print(f"Written    : {silver_table}")