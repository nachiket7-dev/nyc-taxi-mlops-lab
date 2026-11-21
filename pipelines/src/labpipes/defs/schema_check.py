"""S1: the schema_contract check on trip_files.

Compare the landed file's Parquet footer (the DataBucket resource reads it) with
contracts/trips.schema.yaml, before raw_trips copies anything. contracts/platform.yaml says this check
is blocking.
"""

import dagster as dg

from ..contracts import trips_schema
from ..partitions import trip_months
from ..resources import DataBucket
from .trip_files import trip_files


@dg.asset_check(
    asset=trip_files,
    partitions_def=trip_months,
    blocking=True,
    description="Parquet schema matches contracts/trips.schema.yaml in column count, names, and types.",
)
def schema_contract(context: dg.AssetCheckExecutionContext, data_bucket: DataBucket) -> dg.AssetCheckResult:
    month = context.partition_key
    footer = data_bucket.footer(month)
    expected = trips_schema()
    actual = footer.columns

    mismatches = []
    if len(actual) != len(expected):
        mismatches.append(f"Column count mismatch: expected {len(expected)}, got {len(actual)}")
    else:
        for i, (act, exp) in enumerate(zip(actual, expected, strict=True)):
            if act.name.lower() != exp.name.lower() or act.type != exp.type:
                mismatches.append(f"Col {i}: expected {exp.name}:{exp.type}, got {act.name}:{act.type}")

    passed = len(mismatches) == 0
    return dg.AssetCheckResult(
        passed=passed,
        severity=dg.AssetCheckSeverity.ERROR,
        description="Schema matches contract" if passed else "; ".join(mismatches[:3]),
        metadata={
            "mismatches": dg.MetadataValue.json(mismatches),
            "num_columns": dg.MetadataValue.int(len(actual)),
            "num_rows": dg.MetadataValue.int(footer.num_rows),
        },
    )
