"""The ingest job and the sensor that drives it: a new or changed file under trips/ adds its month as a
partition and requests one run for that month, from the footer check through to the mart."""

import json

import dagster as dg

from ..partitions import trip_months
from ..resources import DataBucket

ingest_month = dg.define_asset_job(
    "ingest_month",
    selection=dg.AssetSelection.assets(
        "taxi_zones", "trip_files", "raw_trips", "stg_trips", "zone_hour_stats", "trip_features"
    ),
)


@dg.sensor(job=ingest_month, minimum_interval_seconds=30, default_status=dg.DefaultSensorStatus.RUNNING)
def trip_files_sensor(context: dg.SensorEvaluationContext, data_bucket: DataBucket):
    # Cursor: month -> ETag already requested. A re-uploaded file (new ETag) is loaded again.
    seen = json.loads(context.cursor or "{}")
    files = data_bucket.trip_files()
    new = [f for f in files if seen.get(f.month) != f.etag]
    if not new:
        return dg.SkipReason(f"{len(files)} file(s) under trips/, none new or changed")
    known = set(context.instance.get_dynamic_partitions(trip_months.name))
    return dg.SensorResult(
        run_requests=[dg.RunRequest(partition_key=f.month, run_key=f"{f.month}:{f.etag}") for f in new],
        dynamic_partitions_requests=[trip_months.build_add_request(sorted({f.month for f in new} - known))],
        cursor=json.dumps(seen | {f.month: f.etag for f in new}),
    )
