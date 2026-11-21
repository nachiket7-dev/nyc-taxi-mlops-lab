"""schema_contract: the landed file's Parquet footer against contracts/trips.schema.yaml, before COPY.

Blocking, as contracts/platform.yaml requires: a failure stops raw_trips in the same run, so a file
whose schema moved (E4a's appended column, E8's changed types) is never copied.
"""

import dagster as dg

from ..contracts import Column, trips_schema
from ..partitions import trip_months
from ..resources import DataBucket
from .trip_files import trip_files


def schema_problems(expected: list[Column], actual: list[Column]) -> list[str]:
    """Every difference that would break or corrupt a positional COPY. Names compare case-insensitively."""
    problems = []
    if len(actual) != len(expected):
        problems.append(f"{len(actual)} columns, contract has {len(expected)}")
    for i, (e, a) in enumerate(zip(expected, actual, strict=False), start=1):
        if a.name.lower() != e.name.lower():
            problems.append(f"column {i}: named {a.name}, contract says {e.name}")
        if a.type != e.type:
            problems.append(f"column {i} ({e.name}): {a.type}, contract says {e.type}")
    for i, a in enumerate(actual[len(expected) :], start=len(expected) + 1):
        problems.append(f"column {i}: unexpected {a.name} ({a.type})")
    return problems


@dg.asset_check(
    asset=trip_files,
    partitions_def=trip_months,
    blocking=True,
    description="The file's Parquet schema matches contracts/trips.schema.yaml: checked before any COPY.",
)
def schema_contract(context: dg.AssetCheckExecutionContext, data_bucket: DataBucket) -> dg.AssetCheckResult:
    actual = data_bucket.footer(context.partition_key).columns
    problems = schema_problems(trips_schema(), actual)
    return dg.AssetCheckResult(
        passed=not problems,
        severity=dg.AssetCheckSeverity.ERROR,
        description="; ".join(problems[:5]) or "matches the contract",
        metadata={"problems": dg.MetadataValue.json(problems), "columns": dg.MetadataValue.int(len(actual))},
    )
