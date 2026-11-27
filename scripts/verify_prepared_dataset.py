
import json

import pandas as pd


DATA_PATH = (
    "data/prepared/yellow_taxi_2024_01_23.parquet"
)

MANIFEST_PATH = (
    "data/prepared/dataset_manifest.json"
)


def main():

    # Load the prepared Parquet data.
    df = pd.read_parquet(
        DATA_PATH,
        engine="pyarrow",
    )

    # Load its metadata manifest.
    with open(MANIFEST_PATH, encoding="utf-8") as file:
        manifest = json.load(file)

    print("========== DATASET SHAPE ==========")
    print(df.shape)

    print("\n========== FIRST 5 RECORDS ==========")
    print(df.head().to_string(index=False))

    print("\n========== DATA TYPES ==========")
    print(df.dtypes)

    print("\n========== DATA QUALITY ==========")

    print("Total records:", len(df))

    print(
        "Unique event IDs:",
        df["event_id"].nunique(),
    )

    print(
        "Duplicate event IDs:",
        df["event_id"].duplicated().sum(),
    )

    print(
        "Unique pickup zones:",
        df["pickup_location_id"].nunique(),
    )

    print(
        "Earliest local pickup:",
        df["event_time_local"].min(),
    )

    print(
        "Latest local pickup:",
        df["event_time_local"].max(),
    )

    print(
        "UTC timezone:",
        df["event_time_utc"].dt.tz,
    )

    # Check timezone and local-date requirements.
    assert len(df) == 99891
    assert df["event_id"].is_unique
    assert df["event_time_utc"].dt.tz is not None
    assert str(df["event_time_utc"].dt.tz) == "UTC"

    assert (
        df["event_time_local"].dt.date
        == pd.Timestamp("2024-01-23").date()
    ).all()

    # Recalculate window boundaries independently.
    expected_start = df["event_time_utc"].dt.floor("15min")
    expected_end = expected_start + pd.Timedelta(minutes=15)

    assert (
        df["window_start_utc"] == expected_start
    ).all()

    assert (
        df["window_end_utc"] == expected_end
    ).all()

    assert (
        df["pickup_location_id"].notna()
    ).all()

    print("\n========== MANIFEST ==========")
    print(json.dumps(
        manifest["record_counts"],
        indent=4,
    ))

    print("\nAll validation checks passed!")


if __name__ == "__main__":
    main()
