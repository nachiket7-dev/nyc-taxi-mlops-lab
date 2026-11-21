"""raw_trips: one month's file COPYed into ingest.raw_trips, replacing any earlier load of that file.

The reload is one transaction (temp landing table, COPY, delete, insert), so re-running a month never
duplicates rows and a failed COPY leaves the previous load in place. Every row records its source
file, which is what the delete keys on: TLC files contain a few trips dated outside their month, so a
date-range delete would miss them.

Dist/sort keys (decision record 006): DISTSTYLE AUTO, sorted by pickup time, since every reader
(staging per month, the S2 training window) filters on time.
"""

import dagster as dg

from ..contracts import redshift_columns, trips_schema
from ..partitions import source_file, trip_months
from ..resources import DataBucket, RedshiftData


@dg.asset(partitions_def=trip_months, deps=["trip_files"], group_name="ingest", kinds={"redshift"}, pool="redshift")
def raw_trips(context: dg.AssetExecutionContext, redshift: RedshiftData, data_bucket: DataBucket) -> dg.MaterializeResult:
    month = context.partition_key
    file, uri = source_file(month), data_bucket.trip_uri(month)
    cols = redshift_columns(trips_schema())
    redshift.execute(
        "create schema if not exists ingest",
        f"create table if not exists ingest.raw_trips ({cols}, source_file varchar(64) not null, "
        "loaded_at timestamp default getdate()) diststyle auto sortkey (tpep_pickup_datetime)",
        f"create temp table landing ({cols})",
        f"copy landing from '{uri}' iam_role default format as parquet",
        f"delete from ingest.raw_trips where source_file = '{file}'",
        f"insert into ingest.raw_trips select *, '{file}', getdate() from landing",
    )
    rows = redshift.scalar(f"select count(*) from ingest.raw_trips where source_file = '{file}'")
    return dg.MaterializeResult(metadata={"rows": dg.MetadataValue.int(int(rows)), "source": uri})
