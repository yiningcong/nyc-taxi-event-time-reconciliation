
from pathlib import Path

import pandas as pd


DATA_DIR = Path("data/raw")

PARQUET_PATH = DATA_DIR / "yellow_tripdata_2024-01.parquet"
ZONE_PATH = DATA_DIR / "taxi_zone_lookup.csv"


def main():
    # Read only the two fields needed for this validation.
    df = pd.read_parquet(
        PARQUET_PATH,
        columns=["tpep_pickup_datetime", "PULocationID"],
    )

    # Keep a stable ordinal identifying each record's position
    # in the source Parquet file.
    df.insert(
        0,
        "source_row_number",
        range(1, len(df) + 1),
    )

    # The source timestamps are timezone-naive.
    # These boundaries are expressed as New York local time.
    january_start = pd.Timestamp("2024-01-01 00:00:00")
    february_start = pd.Timestamp("2024-02-01 00:00:00")

    timestamps = df["tpep_pickup_datetime"]

    # January includes its start but excludes February 1.
    in_january = (
        (timestamps >= january_start)
        & (timestamps < february_start)
    )

    before_january = timestamps < january_start
    after_january = timestamps >= february_start

    print("=== DATE RANGE VALIDATION ===")
    print("Total records:", len(df))
    print("Records within January:", int(in_january.sum()))
    print("Records before January:", int(before_january.sum()))
    print("Records on or after February 1:", int(after_january.sum()))

    print("\n=== EARLIEST OUT-OF-RANGE RECORDS ===")

    outside = df.loc[~in_january]

    print(
        outside.sort_values("tpep_pickup_datetime")
        .head(10)
        .to_string(index=False)
    )

    print("\n=== LATEST OUT-OF-RANGE RECORDS ===")

    print(
        outside.sort_values(
            "tpep_pickup_datetime",
            ascending=False,
        )
        .head(10)
        .to_string(index=False)
    )

    # Check the pickup zone IDs against the official lookup.
    zones = pd.read_csv(ZONE_PATH)

    valid_zone_ids = set(
        zones["LocationID"].dropna().astype(int)
    )

    invalid_zone = ~df["PULocationID"].isin(valid_zone_ids)

    january_records = df.loc[in_january]
    invalid_january_zone = ~january_records[
        "PULocationID"
    ].isin(valid_zone_ids)

    print("\n=== ZONE ID VALIDATION ===")
    print("Distinct IDs in full dataset:", df["PULocationID"].nunique())

    print(
        "Distinct valid lookup IDs:",
        len(valid_zone_ids),
    )

    print(
        "Records with unknown zones (full file):",
        int(invalid_zone.sum()),
    )

    print(
        "Records with unknown zones (January):",
        int(invalid_january_zone.sum()),
    )

    print("\nUnknown zone frequencies within January:")

    print(
        january_records.loc[
            invalid_january_zone,
            "PULocationID",
        ].value_counts().to_string()
    )


if __name__ == "__main__":
    main()
