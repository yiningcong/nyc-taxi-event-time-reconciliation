
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq


# Locate the source files
DATA_DIR = Path("data/raw")

PARQUET_PATH = DATA_DIR / "yellow_tripdata_2024-01.parquet"
ZONE_PATH = DATA_DIR / "taxi_zone_lookup.csv"


def main():
    # 1. Inspect a small sample from the first row group
    parquet_file = pq.ParquetFile(PARQUET_PATH)

    sample = (
        parquet_file.read_row_group(
            0,
            columns=[
                "tpep_pickup_datetime",
                "PULocationID",
            ],
        )
        .slice(0, 10)
        .to_pandas()
    )

    print("=== FIRST 10 RECORDS ===")
    print(sample.to_string(index=False))

    # 2. Inspect the two required columns across the file
    df = pd.read_parquet(
        PARQUET_PATH,
        columns=[
            "tpep_pickup_datetime",
            "PULocationID",
        ],
    )

    print("\n=== DATA QUALITY ===")

    print("Total records:", len(df))

    print(
        "Missing pickup timestamps:",
        df["tpep_pickup_datetime"].isna().sum(),
    )

    print(
        "Missing pickup zones:",
        df["PULocationID"].isna().sum(),
    )

    print(
        "Distinct pickup zones:",
        df["PULocationID"].nunique(),
    )

    print(
        "Earliest pickup timestamp:",
        df["tpep_pickup_datetime"].min(),
    )

    print(
        "Latest pickup timestamp:",
        df["tpep_pickup_datetime"].max(),
    )

    print("\n=== PICKUP ZONE FREQUENCIES ===")
    print(df["PULocationID"].value_counts().head(10))

    # 3. Inspect the taxi zone lookup
    zones = pd.read_csv(ZONE_PATH)

    print("\n=== TAXI ZONE LOOKUP ===")
    print(zones.head(10).to_string(index=False))

    print("\nLookup columns:", zones.columns.tolist())
    print("Lookup records:", len(zones))
    print("Unique LocationIDs:", zones["LocationID"].nunique())


if __name__ == "__main__":
    main()
