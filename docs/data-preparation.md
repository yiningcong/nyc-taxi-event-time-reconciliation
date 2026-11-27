
# Dataset Selection and Preparation

## 1. Source Dataset

The experiment uses the NYC TLC Yellow Taxi Trip Records
for January 2024, downloaded in Parquet format.

The experimental period is restricted to 23 January 2024.

Source fields:
- tpep_pickup_datetime
- PULocationID

The taxi zone lookup CSV is used to validate pickup zone IDs.

## 2. Experimental Date Selection

The experimental interval is:

[2024-01-23 00:00:00,
 2024-01-24 00:00:00)

Both boundaries are interpreted in America/New_York
local time. The start is inclusive and the end is exclusive.

## 3. Data Cleaning Rules

1. Exclude records outside the experimental date.
2. Exclude records with missing pickup timestamps.
3. Exclude records with missing pickup zone IDs.
4. Exclude pickup zone IDs absent from the official
   TLC taxi zone lookup.
5. Retain records sharing the same pickup timestamp
   and pickup zone because they may represent
   separate trips.
6. Do not filter using fare, passenger count or
   trip distance because these fields are not used
   in pickup-count aggregation.

## 4. Timestamp Handling

Original pickup timestamps are interpreted as
America/New_York local time.

The original local timestamp is retained.
A timezone-aware UTC timestamp is derived for
Flink event-time processing.

## 5. Event Identification

Each valid record receives a deterministic ID
based on the source filename and its original
row position in the Parquet read sequence.

## 6. Event-Time Windows

Each event belongs to a 15-minute tumbling
event-time window.

Window boundaries use UTC timestamps.
The window start is inclusive and the window
end is exclusive.

## 7. Validation Results

Source records: 2,964,624
Records on 23 January: 99,891
Missing timestamps: 0
Missing pickup zones on selected day: 0
Unknown pickup zones on selected day: 0
Valid experimental records: 99,891
Excluded from experimental extract: 2,864,733

The source-to-extract record accounting check passed.

## 8. Arrival Simulation

Synthetic arrival delays are not generated
during dataset preparation.

The replay component will generate arrival
times separately while preserving the original
event-time timestamp.
