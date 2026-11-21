# 006: Quality checks: blocking vs warning

Stage: S1 · Date: 2026-11-21 · Author: Nachiket Amlekar · Drafted by agent: yes

## Choice

Split data quality validations on `staging.stg_trips` into blocking structural integrity checks (`stg_trips_integrity`) that halt downstream execution, and non-blocking anomaly checks (`stg_trips_anomalies`) that emit warnings.

## Rejected alternative

Making all quality checks blocking, or using soft non-blocking warnings for all schema and data anomalies.

## Why

Corrupted structural records—such as null timestamps (`pickup_at`, `dropoff_at`), non-positive durations (`duration_min <= 0`), or an empty partition—indicate unparseable or fundamentally corrupted data that would produce invalid metrics in `marts.zone_hour_stats`. These must block downstream assets. Conversely, real-world TLC taxi datasets routinely contain sensor noise and statistical outliers (e.g., trips > 180 min, or unknown zone IDs like 264/265). Halting production runs for expected noisy records creates brittle pipelines. We enforce hard failure (0 tolerance) for structural integrity, while setting alert thresholds (< 2% on duration outliers and < 5% on unknown zones) as non-blocking warnings.

## What would show this was wrong

If corrupt records that bypass structural integrity checks corrupt downstream models or mart aggregations without halting the run, or if benign real-world traffic shifts cause persistent anomaly warnings that operational teams routinely ignore.
