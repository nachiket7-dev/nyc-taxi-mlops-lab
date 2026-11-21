"""Data-quality checks on stg_trips, one month at a time.

Thresholds and blocking-vs-warning are decisions (record 005). The reference blocks on what would
corrupt the mart or the model (impossible durations, duplicated trips, rows lost between file and
table) and warns on what real TLC months always have some of. Baselines, 2024-01..04
(scripts/verify/results/tlc-stats-2024-01-04.md): non-positive durations 0.03%, trips over 3 h 0.06%,
exact duplicates ~0, null passenger_count 4.7-11.9%, unknown zones ~1%.
"""

import dagster as dg

from ..partitions import trip_months
from ..resources import RedshiftData
from .staging import stg_trips

MAX_NONPOSITIVE_DURATION = 0.005  # 15x the worst baseline month
MAX_DUPLICATE_ROWS = 0.0001
MAX_OVER_3H = 0.005
MAX_NULL_PASSENGERS = 0.15
MAX_UNKNOWN_ZONE = 0.03
VOLUME_RATIO = (0.5, 2.0)  # rows vs the mean of the other loaded months


def _rates(redshift: RedshiftData, month: str) -> dict:
    # float8, not 1.0: Redshift averages a numeric(2,1) to one decimal place, so 1.03% would read as 0.0.
    return redshift.query(f"""
        select count(*) as rows,
               avg(case when duration_min <= 0 then 1::float8 else 0::float8 end) as nonpositive_duration,
               avg(case when duration_min > 180 then 1::float8 else 0::float8 end) as over_3h,
               avg(case when passenger_count is null then 1::float8 else 0::float8 end) as null_passengers,
               avg(case when pickup_zone_id not between 1 and 263 or dropoff_zone_id not between 1 and 263
                        then 1::float8 else 0::float8 end) as unknown_zone
        from staging.stg_trips where trip_month = '{month}'""")[0]


def _result(checks: dict[str, tuple[float, float]], severity: dg.AssetCheckSeverity) -> dg.AssetCheckResult:
    """checks: name -> (value, max allowed)."""
    over = [f"{k} {v:.4%} > {m:.4%}" for k, (v, m) in checks.items() if v > m]
    return dg.AssetCheckResult(
        passed=not over,
        severity=severity,
        description="; ".join(over) or "within thresholds",
        metadata={k: dg.MetadataValue.float(float(v)) for k, (v, _) in checks.items()},
    )


@dg.asset_check(asset=stg_trips, partitions_def=trip_months, blocking=True,
                description="Non-positive durations stay near the baseline (E5's corrupt month doesn't).")
def durations_valid(context: dg.AssetCheckExecutionContext, redshift: RedshiftData) -> dg.AssetCheckResult:
    r = _rates(redshift, context.partition_key)
    return _result({"nonpositive_duration": (float(r["nonpositive_duration"]), MAX_NONPOSITIVE_DURATION)},
                   dg.AssetCheckSeverity.ERROR)


@dg.asset_check(asset=stg_trips, partitions_def=trip_months, blocking=True,
                description="No duplicated trips: identical rows would double-count in the mart and the model.")
def no_duplicates(context: dg.AssetCheckExecutionContext, redshift: RedshiftData) -> dg.AssetCheckResult:
    m = context.partition_key
    r = redshift.query(f"""
        select (select count(*) from staging.stg_trips where trip_month = '{m}') as rows,
               (select coalesce(sum(n - 1), 0) from (
                  select count(*) as n from staging.stg_trips where trip_month = '{m}'
                  group by vendor_id, pickup_at, dropoff_at, pickup_zone_id, dropoff_zone_id, passenger_count,
                           trip_distance, ratecode_id, payment_type, fare_amount, total_amount
                  having count(*) > 1)) as extra""")[0]
    rate = int(r["extra"]) / max(int(r["rows"]), 1)
    return _result({"duplicate_rows": (rate, MAX_DUPLICATE_ROWS)}, dg.AssetCheckSeverity.ERROR)


@dg.asset_check(asset=stg_trips, partitions_def=trip_months, blocking=True,
                description="Every row in the file reached staging (file footer row count vs table).")
def rows_reconcile(context: dg.AssetCheckExecutionContext, redshift: RedshiftData) -> dg.AssetCheckResult:
    m = context.partition_key
    rec = context.instance.fetch_materializations(
        dg.AssetRecordsFilter(asset_key=dg.AssetKey("trip_files"), asset_partitions=[m]), limit=1
    ).records
    if not rec:
        return dg.AssetCheckResult(passed=False, description=f"no trip_files materialization for {m}")
    in_file = int(rec[0].asset_materialization.metadata["num_rows"].value)
    in_table = int(redshift.scalar(f"select count(*) from staging.stg_trips where trip_month = '{m}'"))
    return dg.AssetCheckResult(
        passed=in_file == in_table,
        description=f"file {in_file:,} rows, staging {in_table:,}",
        metadata={"file_rows": dg.MetadataValue.int(in_file), "table_rows": dg.MetadataValue.int(in_table)},
    )


@dg.asset_check(asset=stg_trips, partitions_def=trip_months,
                description="Completeness within what real TLC months show (warning only).")
def completeness(context: dg.AssetCheckExecutionContext, redshift: RedshiftData) -> dg.AssetCheckResult:
    r = _rates(redshift, context.partition_key)
    return _result({
        "over_3h": (float(r["over_3h"]), MAX_OVER_3H),
        "null_passengers": (float(r["null_passengers"]), MAX_NULL_PASSENGERS),
        "unknown_zone": (float(r["unknown_zone"]), MAX_UNKNOWN_ZONE),
    }, dg.AssetCheckSeverity.WARN)


@dg.asset_check(asset=stg_trips, partitions_def=trip_months,
                description="Row count in line with the other loaded months (warning only).")
def volume(context: dg.AssetCheckExecutionContext, redshift: RedshiftData) -> dg.AssetCheckResult:
    m = context.partition_key
    counts = {r["trip_month"]: int(r["n"]) for r in redshift.query(
        "select trip_month, count(*) as n from staging.stg_trips group by trip_month")}
    others = [n for k, n in counts.items() if k != m]
    if not others:
        return dg.AssetCheckResult(passed=True, description="first month: nothing to compare with")
    ratio = counts.get(m, 0) / (sum(others) / len(others))
    lo, hi = VOLUME_RATIO
    return dg.AssetCheckResult(
        passed=lo <= ratio <= hi,
        severity=dg.AssetCheckSeverity.WARN,
        description=f"{ratio:.2f}x the mean of {len(others)} other month(s)",
        metadata={"ratio": dg.MetadataValue.float(ratio)},
    )
