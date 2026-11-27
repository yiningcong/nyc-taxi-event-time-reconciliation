import duckdb

REFERENCE_FILE = "data/reference/reference_aggregates_2024_01_23.parquet"


def main():
    con = duckdb.connect()

    con.execute("SET TimeZone = 'UTC'")

    print("========== REFERENCE DATA ==========")

    print(
        con.execute(
            f"""
            DESCRIBE
            SELECT *
            FROM read_parquet('{REFERENCE_FILE}')
            """
        )
        .fetchdf()
        .to_string(index=False)
    )

    print("\n========== SUMMARY ==========")

    summary = con.execute(
        f"""
        SELECT
            COUNT(*) AS zone_window_pairs,
            SUM(pickup_count) AS total_pickups,
            COUNT(DISTINCT pickup_location_id) AS zones,
            COUNT(DISTINCT window_start_utc) AS windows,
            AVG(pickup_count) AS average_pickups_per_pair,
            MAX(pickup_count) AS maximum_pickups_per_pair
        FROM read_parquet('{REFERENCE_FILE}')
        """
    )

    print(summary.fetchdf().to_string(index=False))

    print("\n========== FIRST 20 ROWS ==========")

    sample = con.execute(
        f"""
        SELECT *
        FROM read_parquet('{REFERENCE_FILE}')
        ORDER BY window_start_utc, pickup_location_id
        LIMIT 20
        """
    )

    print(sample.fetchdf().to_string(index=False))

    con.close()


if __name__ == "__main__":
    main()
