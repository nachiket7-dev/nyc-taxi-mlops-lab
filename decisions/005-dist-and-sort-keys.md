# 005: Dist and sort keys

Stage: S1 · Date: 2026-11-21 · Author: Nachiket Amlekar · Drafted by agent: yes

## Choice

Configure `ingest.raw_trips` with `DISTSTYLE EVEN` and `SORTKEY (tpep_pickup_datetime)`.

## Rejected alternative

Using `DISTSTYLE KEY` on `pulocationid` or `DISTSTYLE ALL`.

## Why

Our warehouse runs on a single `ra3.large` node in Learner Lab where cross-slice network redistribution is not an issue; `EVEN` avoids data skew without requiring distribution key maintenance. `SORTKEY (tpep_pickup_datetime)` matches the time-based staging transformations, monthly window filtering, and percentile aggregations in downstream marts (`staging.stg_trips` and `marts.zone_hour_stats`). This ordering allows Redshift 1MB block metadata (zone maps) to skip irrelevant date ranges during staging transformations and backfill queries.

## What would show this was wrong

If scaling to a multi-node cluster where frequent joins on `pulocationid` against `taxi_zones` or dimension tables cause broadcast bottlenecks that would benefit from co-locating key distribution (`DISTSTYLE KEY`), or if query patterns predominantly filter on zone IDs rather than temporal windows.
