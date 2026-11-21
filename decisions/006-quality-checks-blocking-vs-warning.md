# 006: Quality checks: blocking vs warning

Stage: S1 · Date: 2026-11-21 · Author: Nachiket Amlekar · Drafted by agent: yes

## Choice

Split data quality validations on `staging.stg_trips` into blocking structural integrity checks (`stg_trips_integrity`) that halt downstream execution, and non-blocking anomaly checks (`stg_trips_anomalies`) that emit warnings.

## Rejected alternative

Making all quality checks blocking, or using soft non-blocking warnings for all schema and data anomalies.

## Why

Corrupted structural records—such as null timestamps (`pickup_at`, `dropoff_at`) or an empty partition—indicate unparseable or fundamentally corrupted data that would produce invalid metrics in `marts.zone_hour_stats`. These must block downstream assets with zero tolerance.

Empirical measurement of real-world NYC TLC Yellow Taxi months shows ~0.03% (870 out of ~3,000,000 trips in 2024-01) with non-positive durations (`duration_min <= 0`), caused by immediate meter aborts or trip recording glitches. Downstream aggregations in `marts.zone_hour_stats` and ML training in `trip_features` explicitly filter out trips with `duration_min <= 0`. Therefore, rather than a brittle 0-tolerance rule that would break valid monthly production ingests, we set a strict blocking threshold of `< 0.1%` (1 in 1,000 trips) on non-positive durations. A corrupted batch (such as inverted timestamps, schema shifts, or blank files) exceeds this threshold by orders of magnitude and is halted.

Conversely, statistical outliers (trips > 180 min, or unknown zones like 264/265) reflect real city conditions. Halting production runs for expected noisy records creates brittle pipelines. We enforce alert thresholds (< 2% on duration outliers and < 5% on unknown zones) as non-blocking warnings.

## What would show this was wrong

If corrupt records that bypass structural integrity checks corrupt downstream models or mart aggregations without halting the run, or if benign real-world traffic shifts cause persistent anomaly warnings that operational teams routinely ignore.
