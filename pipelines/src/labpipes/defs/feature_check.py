"""S2: the feature_contract check on trip_features.

contracts/features.yaml lists the model's inputs, label and the post-trip columns that must never be
inputs; contracts/mlflow.yaml says this check blocks. Decide what "matches" means: column names,
types, value ranges. Gameday E1 will test it.
"""

import dagster as dg

from ..contracts import features_contract
from ..partitions import trip_months
from ..resources import RedshiftData
from .features import trip_features


@dg.asset_check(
    asset=trip_features,
    partitions_def=trip_months,
    blocking=True,
    description="Table columns and value ranges strictly follow contracts/features.yaml: checked before training.",
)
def feature_contract(
    context: dg.AssetCheckExecutionContext,
    redshift: RedshiftData,
) -> dg.AssetCheckResult:
    month = context.partition_key
    contract = features_contract()
    forbidden_cols = set(contract.get("forbidden", []))
    expected_cols = {
        col["name"]
        for section in ("keys", "inputs")
        for col in contract.get(section, [])
    } | {contract["label"]["name"]}

    # 1. Schema check: Verify table column names from information_schema
    cols_query = (
        "select column_name from information_schema.columns "
        "where table_schema = 'staging' and table_name = 'trip_features'"
    )
    actual_cols = {r["column_name"].lower() for r in redshift.query(cols_query)}
    problems = []

    # Check for forbidden post-trip leaky columns
    leaked = actual_cols & {c.lower() for c in forbidden_cols}
    if leaked:
        problems.append(f"Forbidden post-trip columns found: {sorted(leaked)}")

    # Check that actual columns match expected schema
    missing = expected_cols - actual_cols
    if missing:
        problems.append(f"Missing expected columns: {sorted(missing)}")

    extra = actual_cols - expected_cols
    if extra:
        problems.append(f"Unexpected extra columns: {sorted(extra)}")

    # 2. Data check: Row counts and ranges for partition
    stats = redshift.query(f"""
        select count(*) as total,
               count(case when pickup_zone_id not between 1 and 263 then 1 end) as bad_pz,
               count(case when dropoff_zone_id not between 1 and 263 then 1 end) as bad_dz,
               count(case when pickup_hour not between 0 and 23 then 1 end) as bad_hour,
               count(case when pickup_dow not between 0 and 6 then 1 end) as bad_dow,
               count(case when duration_min <= 0 or duration_min > 180 or duration_min is null then 1 end) as bad_dur
        from staging.trip_features
        where trip_month = '{month}'
    """)[0]

    total = int(stats["total"] or 0)
    if total == 0:
        problems.append("Partition has 0 rows")

    bad_rows = (
        int(stats["bad_pz"] or 0)
        + int(stats["bad_dz"] or 0)
        + int(stats["bad_hour"] or 0)
        + int(stats["bad_dow"] or 0)
        + int(stats["bad_dur"] or 0)
    )
    if bad_rows > 0:
        problems.append(f"{bad_rows} row(s) violate contract range constraints")

    passed = len(problems) == 0
    return dg.AssetCheckResult(
        passed=passed,
        severity=dg.AssetCheckSeverity.ERROR,
        description="; ".join(problems) or "Matches features.yaml contract",
        metadata={
            "total_rows": dg.MetadataValue.int(total),
            "bad_rows": dg.MetadataValue.int(bad_rows),
            "columns": dg.MetadataValue.json(sorted(actual_cols)),
        },
    )
