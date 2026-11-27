from __future__ import annotations

import hashlib
import json
from pathlib import Path

import duckdb


# --------------------------------------------------
# 1. Project configuration
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[1]

PREPARED_FILE = PROJECT_ROOT / "data/prepared/yellow_taxi_2024_01_23.parquet"

PREPARATION_MANIFEST = PROJECT_ROOT / "data/prepared/dataset_manifest.json"

OUTPUT_DIR = PROJECT_ROOT / "data/reference"

REFERENCE_FILE = OUTPUT_DIR / "reference_aggregates_2024_01_23.parquet"

REFERENCE_MANIFEST = OUTPUT_DIR / "reference_manifest.json"

WINDOW_SIZE = "15 minutes"

# Explicit UTC origin for the 15-minute window grid.
WINDOW_ORIGIN = "2000-01-03 00:00:00+00"


# --------------------------------------------------
# 2. Utility functions
# --------------------------------------------------
def calculate_sha256(path: Path) -> str:
    """Calculate the SHA-256 checksum of a file."""
    digest = hashlib.sha256()

    with path.open("rb") as file:
        for chunk in iter(
            lambda: file.read(1024 * 1024),
            b"",
        ):
            digest.update(chunk)

    return digest.hexdigest()


def sql_path(path: Path) -> str:
    """Convert a filesystem path into a SQL string literal."""
    escaped = str(path.resolve()).replace("'", "''")
    return f"'{escaped}'"


# --------------------------------------------------
# 3. Validate prepared dataset
# --------------------------------------------------
def validate_prepared_dataset(
    con: duckdb.DuckDBPyConnection,
    preparation_manifest: dict,
) -> dict:
    """Check that the prepared dataset matches its manifests."""
    if not PREPARED_FILE.exists():
        raise FileNotFoundError(f"Prepared dataset not found: {PREPARED_FILE}")

    declared_hash = preparation_manifest["output"]["sha256"]

    actual_hash = calculate_sha256(PREPARED_FILE)

    if actual_hash != declared_hash:
        raise ValueError(
            "Prepared dataset checksum does not match the preparation manifest."
        )

    relation = f"read_parquet({sql_path(PREPARED_FILE)})"

    row = con.execute(
        f"""
        SELECT
            COUNT(*) AS total_records,
            
            COUNT(*) FILTER (
                WHERE event_time_utc IS NULL
            ) AS missing_event_times,
            
            COUNT(*) FILTER (
                WHERE pickup_location_id IS NULL
            ) AS missing_pickup_zones,
            
            COUNT(DISTINCT event_id) AS unique_event_ids,
            
            COUNT(DISTINCT pickup_location_id) AS distinct_zones,
            
            COUNT(*) FILTER (
                WHERE CAST(event_time_local AS DATE)
                    <> DATE '2024-01-23'
            ) AS records_outside_experimental_day
            
        FROM {relation}
        """
    ).fetchone()

    (
        total_records,
        missing_event_times,
        missing_pickup_zones,
        unique_event_ids,
        distinct_zones,
        records_outside_day,
    ) = row

    expected_records = int(preparation_manifest["output"]["records"])

    checks = {
        "record_count_matches_manifest": total_records == expected_records,
        "no_missing_event_times": missing_event_times == 0,
        "no_missing_pickup_zones": missing_pickup_zones == 0,
        "all_event_ids_unique": unique_event_ids == total_records,
        "all_records_on_experimental_day": records_outside_day == 0,
    }
    print("\n========== INPUT VALIDATION ==========")

    for name, passed in checks.items():
        print(f"{name}: {passed}")

    if not all(checks.values()):
        raise ValueError("Prepared dataset validation failed.")
    return {
        "total_records": int(total_records),
        "distinct_zones": int(distinct_zones),
        "unique_event_ids": int(unique_event_ids),
        "validation_checks": checks,
        "sha256": actual_hash,
    }


# --------------------------------------------------
# 4. Independently verify window boundaries
# --------------------------------------------------
def validate_window_boundaries(
    con: duckdb.DuckDBPyConnection,
) -> dict:
    """
    Recalculate windows from event_time_utc in DuckDB
    and compare them with the Pandas-prepared columns.
    """
    relation = f"read_parquet({sql_path(PREPARED_FILE)})"

    row = con.execute(
        f"""
        WITH recalculated AS (
            SELECT
                window_start_utc,
                window_end_utc,

                time_bucket(
                    INTERVAL '{WINDOW_SIZE}',
                    event_time_utc,
                    TIMESTAMPTZ '{WINDOW_ORIGIN}'
                ) AS duckdb_window_start

            FROM {relation}
        )
        
        SELECT
            COUNT(*) FILTER (
                WHERE window_start_utc
                      IS DISTINCT FROM duckdb_window_start
            ) AS start_mismatches,

            COUNT(*) FILTER (
                WHERE window_end_utc
                      IS DISTINCT FROM
                      duckdb_window_start
                      + INTERVAL '{WINDOW_SIZE}'
            ) AS end_mismatches

        FROM recalculated
        """
    ).fetchone()

    start_mismatches = int(row[0])
    end_mismatches = int(row[1])

    checks = {
        "window_start_matches_duckdb": start_mismatches == 0,
        "window_end_matches_duckdb": end_mismatches == 0,
    }

    print("\n========== WINDOW VALIDATION ==========")

    for name, passed in checks.items():
        print(f"{name}: {passed}")

    if not all(checks.values()):
        raise ValueError("Pandas and DuckDB window boundaries differ.")

    return {
        "window_start_mismatches": start_mismatches,
        "window_end_mismatches": end_mismatches,
        "validation_checks": checks,
    }


# --------------------------------------------------
# 5. Build independent reference aggregates
# --------------------------------------------------
def build_aggregates(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """Calculate one reference count per zone and window."""

    relation = f"read_parquet({sql_path(PREPARED_FILE)})"

    query = f"""
        WITH bucketed_events AS (
            
            SELECT
                pickup_location_id,
                
                time_bucket(
                    INTERVAL '{WINDOW_SIZE}',
                    event_time_utc,
                    TIMESTAMPTZ '{WINDOW_ORIGIN}'
                ) AS window_start_utc
            
            FROM {relation}
        )
        
        SELECT
            pickup_location_id,

            window_start_utc,

            window_start_utc
                + INTERVAL '{WINDOW_SIZE}'
                AS window_end_utc,

            COUNT(*)::BIGINT AS pickup_count

        FROM bucketed_events

        GROUP BY
            pickup_location_id,
            window_start_utc

        ORDER BY
            window_start_utc,
            pickup_location_id
    """

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    # Remove an earlier generated result before rebuilding.
    REFERENCE_FILE.unlink(missing_ok=True)

    # DuckDB writes the aggregate directly to Parquet.
    con.execute(
        f"""
        COPY({query})
        TO {sql_path(REFERENCE_FILE)}
        (FORMAT PARQUET, COMPRESSION SNAPPY)
        """
    )

    print("\nReference aggregation completed.")


# --------------------------------------------------
# 6. Validate reference results
# --------------------------------------------------
def validate_reference(
    con: duckdb.DuckDBPyConnection,
    input_records: int,
) -> dict:
    """Verify reference counts and calculate summary metrics."""

    relation = f"read_parquet({sql_path(REFERENCE_FILE)})"

    row = con.execute(
        f"""
        SELECT
            COUNT(*) AS reference_pairs,

            SUM(pickup_count) AS total_pickups,

            COUNT(DISTINCT pickup_location_id)
                AS distinct_zones,

            COUNT(DISTINCT window_start_utc)
                AS distinct_windows,

            MIN(window_start_utc) AS earliest_window,

            MAX(window_start_utc) AS latest_window,

            MIN(pickup_count) AS minimum_pickups,

            MAX(pickup_count) AS maximum_pickups

        FROM {relation}
        """
    ).fetchone()

    (
        reference_pairs,
        total_pickups,
        distinct_zones,
        distinct_windows,
        earliest_window,
        latest_window,
        minimum_pickups,
        maximum_pickups,
    ) = row

    checks = {
        "reference_not_empty": reference_pairs > 0,
        "total_pickups_match_input": total_pickups == input_records,
        "all_reference_counts_positive": minimum_pickups >= 1,
    }

    print("\n========== REFERENCE VALIDATION ==========")

    for name, passed in checks.items():
        print(f"{name}: {passed}")

    if not all(checks.values()):
        raise ValueError("Reference aggregate validation failed.")

    return {
        "reference_pairs": int(reference_pairs),
        "total_pickups": int(total_pickups),
        "distinct_zones": int(distinct_zones),
        "distinct_windows": int(distinct_windows),
        "earliest_window_utc": earliest_window.isoformat(),
        "latest_window_utc": latest_window.isoformat(),
        "minimum_pickups_per_pair": int(minimum_pickups),
        "maximum_pickups_per_pair": int(maximum_pickups),
        "validation_checks": checks,
    }


# --------------------------------------------------
# 7. Save reference manifest
# --------------------------------------------------
def save_reference_manifest(
    input_summary: dict,
    window_summary: dict,
    reference_summary: dict,
) -> None:
    """Save metadata describing the reference dataset."""

    preparation_manifest_hash = calculate_sha256(PREPARATION_MANIFEST)
    reference_hash = calculate_sha256(REFERENCE_FILE)

    manifest = {
        "experiment_date": "2024-01-23",
        "aggregation": {
            "engine": "DuckDB",
            "duckdb_version": duckdb.__version__,
            "input_file": PREPARED_FILE.name,
            "input_sha256": input_summary["sha256"],
            "group_by": [
                "pickup_location_id",
                "window_start_utc",
            ],
            "timestamp_column": "event_time_utc",
            "window_type": "tumbling",
            "window_size": WINDOW_SIZE,
            "window_origin_utc": WINDOW_ORIGIN,
            "aggregation_function": "COUNT(*)",
            "empty_zone_window_pairs": "not emitted",
        },
        "preparation_manifest_sha256": preparation_manifest_hash,
        "input_summary": input_summary,
        "window_validation": window_summary,
        "reference_summary": reference_summary,
        "output": {
            "filename": REFERENCE_FILE.name,
            "sha256": reference_hash,
            "format": "Parquet",
        },
    }

    with REFERENCE_MANIFEST.open(
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(
            manifest,
            file,
            indent=4,
        )

    print("\nReference manifest saved.")


# --------------------------------------------------
# 8. Main execution
# --------------------------------------------------


def main() -> None:
    print("========== DUCKDB REFERENCE AGGREGATION ==========")

    if not PREPARATION_MANIFEST.exists():
        raise FileNotFoundError("Preparation manifest not found.")

    with PREPARATION_MANIFEST.open(encoding="utf-8") as file:
        preparation_manifest = json.load(file)

    with duckdb.connect(database=":memory:") as con:
        # Use UTC for timestamp display and processing.
        con.execute("SET TimeZone = 'UTC'")

        # Validate source data and preparation metadata.
        input_summary = validate_prepared_dataset(
            con,
            preparation_manifest,
        )

        # Independently cross-check Pandas window calculation.
        window_summary = validate_window_boundaries(con)

        # Calculate reference counts.
        print("\nBuilding reference aggregates...")

        build_aggregates(con)

        # Validate aggregate output.
        reference_summary = validate_reference(con, input_summary["total_records"])

        # Display a small sample.
        relation = f"read_parquet({sql_path(REFERENCE_FILE)})"

        sample = con.execute(
            f"""
            SELECT *
            FROM {relation}
            ORDER BY
                window_start_utc,
                pickup_location_id
            LIMIT 10
            """
        ).fetchdf()

        print("\n========== FIRST 10 REFERENCE ROWS ==========")
        print(sample.to_string(index=False))

    # Save metadata after successful validation.
    save_reference_manifest(
        input_summary,
        window_summary,
        reference_summary,
    )

    print("\n========== FINAL REFERENCE SUMMARY ==========")

    for key, value in reference_summary.items():
        if key != "validation_checks":
            print(f"{key}: {value}")

    print("\nReference aggregation finished successfully.")
    print("Output:", REFERENCE_FILE)
    print("Manifest:", REFERENCE_MANIFEST)


if __name__ == "__main__":
    main()
