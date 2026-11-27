# Read the original Parquet source and taxi-zone lookup.
# Select valid records for 23 January 2024.
# Generate stable event IDs.
# Localize New York timestamps and convert them into UTC.
# Calculate window_start_utc and window_end_utc.
# Write a frozen Parquet dataset.
# Generate a dataset manifest describing source, transformations and record counts.

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd

# --------------------------------------------------
# 1. Project configuration
# --------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]

SOURCE_FILE = PROJECT_ROOT / "data/raw/yellow_tripdata_2024-01.parquet"
ZONE_FILE = PROJECT_ROOT / "data/raw/taxi_zone_lookup.csv"

OUTPUT_DIR = PROJECT_ROOT / "data/prepared"
OUTPUT_FILE = OUTPUT_DIR / "yellow_taxi_2024_01_23.parquet"

MANIFEST_FILE = OUTPUT_DIR / "dataset_manifest.json"

EXPERIMENT_DATE = "2024-01-23"
LOCAL_TIMEZONE = "America/New_York"

DAY_START = pd.Timestamp(f"{EXPERIMENT_DATE} 00:00:00")
NEXT_DAY_START = DAY_START + pd.Timedelta(days=1)

WINDOW_SIZE = "15min"


# --------------------------------------------------
# 2. Utility: calculate file checksum
# --------------------------------------------------


def calculate_sha256(path: Path) -> str:
    """Return the SHA-256 checksum of a file."""
    digest = hashlib.sha256()

    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


# --------------------------------------------------
# 3. Read source data
# --------------------------------------------------
def load_source_data() -> tuple[pd.DataFrame, set[int]]:
    """Load the required TLC columns and valid taxi zone IDs."""
    if not SOURCE_FILE.exists():
        raise FileNotFoundError(f"Source Parquet not found: {SOURCE_FILE}")
    if not ZONE_FILE.exists():
        raise FileNotFoundError(f"Taxi zone lookup not found: {ZONE_FILE}")

    # read only columns required for this experiments.
    df = pd.read_parquet(
        SOURCE_FILE, columns=["tpep_pickup_datetime", "PULocationID"], engine="pyarrow"
    )

    # Assign row positions before filtering.
    # These positions support reproducible event IDs.
    df.insert(0, "source_row_number", range(1, len(df) + 1))

    zones = pd.read_csv(ZONE_FILE)
    valid_zone_ids = set(
        pd.to_numeric(zones["LocationID"], errors="coerce").dropna().astype(int)
    )
    return df, valid_zone_ids


# --------------------------------------------------
# 4. Select and validate experimental records
# --------------------------------------------------


def select_experimental_records(
    df: pd.DataFrame,
    valid_zone_ids: set[int],
) -> tuple[pd.DataFrame, dict[str, int]]:
    """Select valid records for the experimental day."""

    timestamps = df["tpep_pickup_datetime"]
    zones = df["PULocationID"]

    # Date classification uses original local timestamps.
    missing_timestamp = timestamps.isna()

    before_day = timestamps.notna() & (timestamps < DAY_START)

    after_day = timestamps.notna() & (timestamps >= NEXT_DAY_START)

    in_experimental_day = (
        timestamps.notna() & (timestamps >= DAY_START) & (timestamps < NEXT_DAY_START)
    )

    day_records = df.loc[in_experimental_day].copy()

    # Validate zones only within the selected date.
    missing_zone = day_records["PULocationID"].isna()

    unknown_zone = day_records["PULocationID"].notna() & ~day_records[
        "PULocationID"
    ].isin(valid_zone_ids)

    valid_zone = day_records["PULocationID"].notna() & day_records["PULocationID"].isin(
        valid_zone_ids
    )

    prepared_source = day_records.loc[valid_zone].copy()

    counts = {
        "source_records": len(df),
        "missing_timestamps": int(missing_timestamp.sum()),
        "before_experimental_day": int(before_day.sum()),
        "on_or_after_next_day": int(after_day.sum()),
        "records_on_experimental_day": len(day_records),
        "missing_zones_on_experimental_day": int(missing_zone.sum()),
        "unknown_zones_on_experimental_day": int(unknown_zone.sum()),
        "valid_records": len(prepared_source),
    }

    counts["excluded_from_extract"] = counts["source_records"] - counts["valid_records"]

    # Make sure every source record is accounted for.
    accounted_records = (
        counts["missing_timestamps"]
        + counts["before_experimental_day"]
        + counts["on_or_after_next_day"]
        + counts["missing_zones_on_experimental_day"]
        + counts["unknown_zones_on_experimental_day"]
        + counts["valid_records"]
    )

    if accounted_records != counts["source_records"]:
        raise ValueError("Record accounting failed. Check the exclusion categories.")

    return prepared_source, counts


# --------------------------------------------------
# 5. Transform event records
# --------------------------------------------------


def transform_events(df: pd.DataFrame) -> pd.DataFrame:
    """Build the standardised event dataset."""

    # Preserve original local pickup time.
    event_time_local = df["tpep_pickup_datetime"]

    # Interpret naive source timestamps as New York local time,
    # then convert them to timezone-aware UTC timestamps.
    event_time_utc = event_time_local.dt.tz_localize(
        LOCAL_TIMEZONE, ambiguous="raise", nonexistent="raise"
    ).dt.tz_convert("UTC")

    # Calculate UTC 15-minute tumbling-window boundaries.
    window_start = event_time_utc.dt.floor(WINDOW_SIZE)
    window_end = window_start + pd.Timedelta(WINDOW_SIZE)

    # Stable IDs use source filename + original row position.
    event_ids = SOURCE_FILE.name + ":" + df["source_row_number"].astype(str)

    prepared = pd.DataFrame(
        {
            "event_id": event_ids,
            "source_row_number": df["source_row_number"],
            "event_time_local": event_time_local,
            "event_time_utc": event_time_utc,
            "pickup_location_id": df["PULocationID"].astype("int32"),
            "window_start_utc": window_start,
            "window_end_utc": window_end,
        }
    )

    return prepared


# --------------------------------------------------
# 6. Save prepared Parquet and manifest
# --------------------------------------------------
def save_outputs(
    prepared: pd.DataFrame,
    counts: dict[str, int],
) -> None:
    """Save the frozen dataset and its metadata manifest."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    prepared.to_parquet(
        OUTPUT_FILE, index=False, engine="pyarrow", compression="snappy"
    )

    # Calculate the checksum for reproducibility
    source_hash = calculate_sha256(SOURCE_FILE)
    output_hash = calculate_sha256(OUTPUT_FILE)

    manifest = {
        "experiment_date": EXPERIMENT_DATE,
        "source": {
            "filename": SOURCE_FILE.name,
            "sha256": source_hash,
            "records": counts["source_records"],
        },
        "preparation": {
            "pickup_timestamp_column": "tpep_pickup_datetime",
            "pickup_zone_column": "PULocationID",
            "local_timezone": LOCAL_TIMEZONE,
            "event_time_timezone": "UTC",
            "window_size": WINDOW_SIZE,
            "window_type": "tumbling",
            "start_boundary": "inclusive",
            "end_boundary": "exclusive",
            "identifier_method": "source filename and source row number",
            "deduplication": "none",
        },
        "record_counts": counts,
        "output": {
            "filename": OUTPUT_FILE.name,
            "records": len(prepared),
            "sha256": output_hash,
            "columns": list(prepared.columns),
            "distinct_pickup_zones": int(prepared["pickup_location_id"].nunique()),
            "earliest_local_event": (prepared["event_time_local"].min().isoformat()),
            "latest_local_event": (prepared["event_time_local"].max().isoformat()),
        },
    }

    with MANIFEST_FILE.open("w", encoding="utf-8") as file:
        json.dump(manifest, file, indent=4)

    print("\n========== PREPARATION COMPLETE ==========")
    print("Valid records:", len(prepared))
    print("Distinct pickup zones:", prepared["pickup_location_id"].nunique())
    print("Prepared Parquet:", OUTPUT_FILE)
    print("Dataset manifest:", MANIFEST_FILE)
    print("Prepared file SHA-256:", output_hash)


# --------------------------------------------------
# 7. Main execution
# --------------------------------------------------


def main() -> None:

    print("Loading source data...")

    df, valid_zone_ids = load_source_data()

    print("Source records:", len(df))

    print("\nSelecting experimental records...")

    selected, counts = select_experimental_records(
        df,
        valid_zone_ids,
    )

    print("\nRecord counts:")
    for name, value in counts.items():
        print(f"{name}: {value}")

    print("\nTransforming events...")

    prepared = transform_events(selected)

    if prepared.empty:
        raise ValueError("No valid experimental records were produced.")

    if prepared["event_id"].duplicated().any():
        raise ValueError("Duplicate event IDs detected.")

    if (
        not (prepared["window_end_utc"] - prepared["window_start_utc"])
        .eq(pd.Timedelta(minutes=15))
        .all()
    ):
        raise ValueError("Invalid window boundaries detected.")

    print("\nSaving prepared dataset...")

    save_outputs(prepared, counts)


if __name__ == "__main__":
    main()
