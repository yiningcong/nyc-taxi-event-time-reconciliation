
from pathlib import Path

import pandas as pd


# --------------------------------------------------
# 1. File locations
# --------------------------------------------------

DATA_DIR = Path("data/raw")

PARQUET_PATH = DATA_DIR / "yellow_tripdata_2024-01.parquet"
ZONE_PATH = DATA_DIR / "taxi_zone_lookup.csv"


# --------------------------------------------------
# 2. Experimental date boundaries
# --------------------------------------------------

DAY_START = pd.Timestamp("2024-01-23 00:00:00")
NEXT_DAY_START = pd.Timestamp("2024-01-24 00:00:00")


def main():

    # Read only the two columns required for this study.
    df = pd.read_parquet(
        PARQUET_PATH,
        columns=[
            "tpep_pickup_datetime",
            "PULocationID",
        ],
    )

    # Assign a stable ordinal based on source-file row order.
    # This will help us generate event IDs later.
    df.insert(
        0,
        "source_row_number",
        range(1, len(df) + 1),
    )

    timestamps = df["tpep_pickup_datetime"]

    # --------------------------------------------------
    # 3. Classify records by pickup date
    # --------------------------------------------------

    has_timestamp = timestamps.notna()

    is_experimental_day = (
        has_timestamp
        & (timestamps >= DAY_START)
        & (timestamps < NEXT_DAY_START)
    )

    is_before_day = (
        has_timestamp
        & (timestamps < DAY_START)
    )

    is_after_day = (
        has_timestamp
        & (timestamps >= NEXT_DAY_START)
    )

    is_missing_timestamp = ~has_timestamp

    # --------------------------------------------------
    # 4. Validate pickup zone IDs
    # --------------------------------------------------

    zones = pd.read_csv(ZONE_PATH)

    valid_zone_ids = set(
        pd.to_numeric(
            zones["LocationID"],
            errors="coerce",
        )
        .dropna()
        .astype(int)
    )

    zone_ids = df["PULocationID"]

    has_valid_zone = zone_ids.isin(valid_zone_ids)

    missing_zone = (
        is_experimental_day
        & zone_ids.isna()
    )

    unknown_zone = (
        is_experimental_day
        & zone_ids.notna()
        & ~has_valid_zone
    )

    valid_records = (
        is_experimental_day
        & has_valid_zone
    )

    # --------------------------------------------------
    # 5. Calculate record counts
    # --------------------------------------------------

    total_records = len(df)

    day_records = int(is_experimental_day.sum())

    missing_timestamps = int(is_missing_timestamp.sum())

    before_day = int(is_before_day.sum())

    after_day = int(is_after_day.sum())

    missing_zones = int(missing_zone.sum())

    unknown_zones = int(unknown_zone.sum())

    valid_count = int(valid_records.sum())

    invalid_zone_count = missing_zones + unknown_zones

    excluded_count = (
        missing_timestamps
        + before_day
        + after_day
        + invalid_zone_count
    )

    # --------------------------------------------------
    # 6. Print validation summary
    # --------------------------------------------------

    print("\n========== EXPERIMENTAL DAY ==========")

    print("Experimental date: 23 January 2024")

    print("Total source records:", total_records)

    print("Records on experimental day:", day_records)

    print("\n========== DATE EXCLUSIONS ==========")

    print("Missing pickup timestamps:", missing_timestamps)

    print("Before 23 January:", before_day)

    print("On or after 24 January:", after_day)

    print("\n========== ZONE VALIDATION ==========")

    print("Missing pickup zones on experimental day:", missing_zones)

    print("Unknown pickup zones on experimental day:", unknown_zones)

    print("Valid pickup records:", valid_count)

    print("\n========== FINAL COUNTS ==========")

    print("Total excluded records:", excluded_count)

    print("Valid prepared records:", valid_count)

    print(
        "Reconciliation check:",
        valid_count + excluded_count == total_records,
    )

    # --------------------------------------------------
    # 7. Inspect records close to date boundaries
    # --------------------------------------------------

    print("\n========== RECORDS BEFORE THE DAY ==========")

    print(
        df.loc[is_before_day]
        .sort_values("tpep_pickup_datetime")
        .tail(5)
        .to_string(index=False)
    )

    print("\n========== RECORDS AFTER THE DAY ==========")

    print(
        df.loc[is_after_day]
        .sort_values("tpep_pickup_datetime")
        .head(5)
        .to_string(index=False)
    )

    # --------------------------------------------------
    # 8. List unknown zone IDs
    # --------------------------------------------------

    print("\n========== UNKNOWN ZONE FREQUENCIES ==========")

    print(
        df.loc[unknown_zone, "PULocationID"]
        .value_counts()
        .to_string()
    )


if __name__ == "__main__":
    main()
