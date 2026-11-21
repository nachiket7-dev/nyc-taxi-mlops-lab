"""S1: data-quality checks on stg_trips (nulls, ranges, duplicates, row counts).

Decision points: which checks block downstream and which only warn, and every threshold. Base them on
what real months look like and on what gameday's corrupt month would look like.
"""

import dagster as dg

from ..partitions import trip_months
from ..resources import RedshiftData
from .staging import stg_trips


@dg.asset_check(
    asset=stg_trips,
    partitions_def=trip_months,
    blocking=True,
    description="Timestamps must not be null, and non-positive durations must be < 0.1% (blocking).",
)
def stg_trips_integrity(
    context: dg.AssetCheckExecutionContext,
    redshift: RedshiftData,
) -> dg.AssetCheckResult:
    month = context.partition_key
    null_timestamps = redshift.scalar(
        f"select count(*) from staging.stg_trips where trip_month = '{month}' "
        "and (pickup_at is null or dropoff_at is null)"
    )
    non_pos_duration = redshift.scalar(
        f"select count(*) from staging.stg_trips where trip_month = '{month}' and duration_min <= 0"
    )
    total = redshift.scalar(f"select count(*) from staging.stg_trips where trip_month = '{month}'")

    null_n = int(null_timestamps or 0)
    non_pos_n = int(non_pos_duration or 0)
    total_n = int(total or 0)

    # Real TLC data has ~0.03% zero-duration trips (meter slips filtered out by zone_hour_stats).
    # Block if null timestamps exist, partition is empty, or invalid duration rate >= 0.1%.
    non_pos_pct = (non_pos_n / total_n) if total_n > 0 else 1.0
    passed = total_n > 0 and null_n == 0 and non_pos_pct < 0.001

    return dg.AssetCheckResult(
        passed=passed,
        severity=dg.AssetCheckSeverity.ERROR,
        description=f"Null timestamps: {null_n}, non-positive duration: {non_pos_n} ({non_pos_pct * 100:.3f}%) out of {total_n}",
        metadata={
            "null_timestamp_rows": dg.MetadataValue.int(null_n),
            "non_positive_duration_rows": dg.MetadataValue.int(non_pos_n),
            "total_rows": dg.MetadataValue.int(total_n),
        },
    )


@dg.asset_check(
    asset=stg_trips,
    partitions_def=trip_months,
    blocking=False,
    description="Warn if outlier durations or out-of-range zones exceed noise thresholds.",
)
def stg_trips_anomalies(
    context: dg.AssetCheckExecutionContext,
    redshift: RedshiftData,
) -> dg.AssetCheckResult:
    month = context.partition_key
    extreme_duration = redshift.scalar(
        f"select count(*) from staging.stg_trips where trip_month = '{month}' and duration_min > 180"
    )
    unknown_zones = redshift.scalar(
        f"select count(*) from staging.stg_trips where trip_month = '{month}' "
        "and (pickup_zone_id not between 1 and 263 or dropoff_zone_id not between 1 and 263)"
    )
    total = redshift.scalar(f"select count(*) from staging.stg_trips where trip_month = '{month}'")

    ext_n = int(extreme_duration or 0)
    unk_n = int(unknown_zones or 0)
    total_n = max(int(total or 1), 1)

    # Warn if extreme durations exceed 2% or unknown zones exceed 5%
    ext_pct = (ext_n / total_n) * 100
    unk_pct = (unk_n / total_n) * 100
    passed = ext_pct < 2.0 and unk_pct < 5.0

    return dg.AssetCheckResult(
        passed=passed,
        severity=dg.AssetCheckSeverity.WARN,
        description=f"Outliers: {ext_n} long trips ({ext_pct:.2f}%), {unk_n} unknown zones ({unk_pct:.2f}%)",
        metadata={
            "extreme_duration_rows": dg.MetadataValue.int(ext_n),
            "unknown_zone_rows": dg.MetadataValue.int(unk_n),
            "total_rows": dg.MetadataValue.int(total_n),
        },
    )
