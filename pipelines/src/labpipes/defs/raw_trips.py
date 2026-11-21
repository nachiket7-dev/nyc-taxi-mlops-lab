"""S1: the raw_trips asset (and the COPY that loads it).

Load one month into ingest.raw_trips idempotently: re-running a month must never leave duplicate rows.
Redshift with the LabRole attached to the cluster (`iam_role default`), so that re-running a month never
duplicates its rows. The landing columns come from contracts/trips.schema.yaml
(contracts.redshift_columns). Decision points: COPY vs Spectrum; dist and sort keys.
"""

import dagster as dg

from ..contracts import redshift_columns, trips_schema
from ..partitions import source_file, trip_months
from ..resources import DataBucket, RedshiftData


@dg.asset(
    partitions_def=trip_months,
    deps=["trip_files"],
    group_name="ingest",
    kinds={"redshift"},
    pool="redshift",
)
def raw_trips(
    context: dg.AssetExecutionContext,
    data_bucket: DataBucket,
    redshift: RedshiftData,
) -> dg.MaterializeResult:
    month = context.partition_key
    file_name = source_file(month)
    s3_uri = data_bucket.trip_uri(month)
    cols = redshift_columns(trips_schema())
    col_names = ", ".join(c.name.lower() for c in trips_schema())

    create_schema_sql = "create schema if not exists ingest;"
    create_table_sql = f"""
    create table if not exists ingest.raw_trips (
      source_file varchar(64),
      {cols}
    )
    diststyle even
    sortkey (tpep_pickup_datetime);
    """
    delete_sql = f"delete from ingest.raw_trips where source_file = '{file_name}';"
    copy_sql = f"""
    copy ingest.raw_trips ({col_names})
    from '{s3_uri}'
    iam_role default
    format as parquet;
    """
    ensure_source_sql = f"update ingest.raw_trips set source_file = '{file_name}' where source_file is null or source_file = '';"

    redshift.execute(
        create_schema_sql,
        create_table_sql,
        delete_sql,
        copy_sql,
        ensure_source_sql,
    )

    count = redshift.scalar(f"select count(*) from ingest.raw_trips where source_file = '{file_name}'")
    return dg.MaterializeResult(
        metadata={
            "source_file": dg.MetadataValue.text(file_name),
            "uri": dg.MetadataValue.text(s3_uri),
            "rows": dg.MetadataValue.int(int(count)),
        }
    )
