"""S1: the S3 sensor (and the job it starts).

Watch trips/ in the data bucket (DataBucket.trip_files). When a month's file appears or changes, add
the month to the trip_months partitions and request a run that goes from trip_files to zone_hour_stats.
Patterns: pipelines/reference/patterns.py.
"""

import json

import dagster as dg

from ..partitions import trip_months
from ..resources import DataBucket

trips_ingest_job = dg.define_asset_job(
    "trips_ingest_job",
    selection=(
        dg.AssetSelection.assets("trip_files").downstream()
        & (dg.AssetSelection.assets("zone_hour_stats").upstream() | dg.AssetSelection.assets("zone_hour_stats"))
    ),
)


@dg.sensor(job=trips_ingest_job, minimum_interval_seconds=60)
def trip_files_sensor(context: dg.SensorEvaluationContext, data_bucket: DataBucket):
    raw_cursor = context.cursor
    cursor_data = {}
    if raw_cursor:
        try:
            parsed = json.loads(raw_cursor)
            if isinstance(parsed, dict):
                cursor_data = parsed
            elif isinstance(parsed, list):
                cursor_data = {m: "" for m in parsed}
        except Exception:
            cursor_data = {}

    current_files = {f.month: f.etag for f in data_bucket.trip_files()}
    new_or_updated = sorted(m for m, etag in current_files.items() if cursor_data.get(m) != etag)

    if not new_or_updated:
        return dg.SkipReason("No new or modified trip files found in data bucket.")

    partitions_to_add = []
    if context.instance:
        existing = set(context.instance.get_dynamic_partitions(trip_months.name))
        partitions_to_add = [m for m in new_or_updated if m not in existing]
    else:
        partitions_to_add = new_or_updated

    dynamic_partitions_requests = []
    if partitions_to_add:
        dynamic_partitions_requests.append(trip_months.build_add_request(partitions_to_add))

    run_requests = [
        dg.RunRequest(
            partition_key=m,
            run_key=f"{m}:{current_files[m]}",
        )
        for m in new_or_updated
    ]

    new_cursor = {**cursor_data, **{m: current_files[m] for m in new_or_updated}}

    return dg.SensorResult(
        run_requests=run_requests,
        dynamic_partitions_requests=dynamic_partitions_requests,
        cursor=json.dumps(new_cursor),
    )
