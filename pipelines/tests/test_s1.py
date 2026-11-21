"""Reference tests for S1: the pre-COPY schema check, the sensor, and the blocking order in a run."""

import json

import dagster as dg
from fakes import LOG, fake_bucket, fake_redshift, schema_2023, schema_2025

from labpipes.contracts import trips_schema
from labpipes.defs.marts import zone_hour_stats
from labpipes.defs.quality import completeness, durations_valid, no_duplicates, rows_reconcile, volume
from labpipes.defs.raw_trips import raw_trips
from labpipes.defs.schema_check import schema_contract, schema_problems
from labpipes.defs.sensor import trip_files_sensor
from labpipes.defs.staging import stg_trips
from labpipes.defs.trip_files import trip_files
from labpipes.defs.zones import taxi_zones
from labpipes.partitions import trip_months


def test_contract_schema_passes():
    assert schema_problems(trips_schema(), trips_schema()) == []


def test_appended_column_fails_e4a():
    problems = schema_problems(trips_schema(), schema_2025())
    assert "20 columns, contract has 19" in problems[0]
    assert any("unexpected cbd_congestion_fee" in p for p in problems)


def test_changed_types_fail_e8_but_casing_alone_does_not():
    problems = schema_problems(trips_schema(), schema_2023())
    assert any("passenger_count): DOUBLE, contract says INT64" in p for p in problems)
    assert not any("airport_fee" in p.lower() and "named" in p for p in problems)


def run_month(month: str, files: dict, redshift=None):
    redshift = redshift or fake_redshift()
    instance = dg.DagsterInstance.ephemeral()
    instance.add_dynamic_partitions(trip_months.name, [month])
    result = dg.materialize(
        [taxi_zones, trip_files, raw_trips, stg_trips, zone_hour_stats,
         schema_contract, durations_valid, no_duplicates, rows_reconcile, completeness, volume],
        partition_key=month,
        instance=instance,
        resources={"redshift": redshift, "data_bucket": fake_bucket(files)},
        raise_on_error=False,
    )
    return result, LOG


def test_good_month_reaches_the_mart():
    result, rs = run_month("2024-01", {"2024-01": ("e1", trips_schema())})
    assert result.success
    assert any(s.startswith("copy landing from 's3://data/trips/yellow_tripdata_2024-01.parquet'") for s in rs)
    assert any("insert into marts.zone_hour_stats" in s for s in rs)


def test_schema_failure_blocks_copy():
    result, rs = run_month("2025-01", {"2025-01": ("e1", schema_2025())})
    assert not result.success
    assert not any(s.startswith("copy landing") for s in rs)
    assert not any("marts.zone_hour_stats" in s for s in rs)


def test_row_mismatch_blocks_the_mart():
    result, rs = run_month("2024-01", {"2024-01": ("e1", trips_schema())}, fake_redshift(rows=2))  # file has 3
    assert not result.success
    assert any(s.startswith("copy landing") for s in rs)
    assert not any("insert into marts.zone_hour_stats" in s for s in rs)


def test_sensor_requests_new_and_changed_files_once():
    instance = dg.DagsterInstance.ephemeral()
    bucket = fake_bucket({"2024-01": ("a", trips_schema()), "2024-02": ("b", trips_schema())})
    ctx = dg.build_sensor_context(instance=instance, resources={"data_bucket": bucket})
    first = trip_files_sensor(ctx)
    assert sorted(r.partition_key for r in first.run_requests) == ["2024-01", "2024-02"]
    assert first.dynamic_partitions_requests[0].partition_keys == ["2024-01", "2024-02"]

    instance.add_dynamic_partitions(trip_months.name, ["2024-01", "2024-02"])
    again = trip_files_sensor(dg.build_sensor_context(instance=instance, cursor=first.cursor, resources={"data_bucket": bucket}))
    assert isinstance(again, dg.SkipReason)

    bucket = fake_bucket({"2024-01": ("a2", trips_schema()), "2024-02": ("b", trips_schema())})
    changed = trip_files_sensor(dg.build_sensor_context(instance=instance, cursor=first.cursor, resources={"data_bucket": bucket}))
    assert [r.run_key for r in changed.run_requests] == ["2024-01:a2"]
    assert changed.dynamic_partitions_requests[0].partition_keys == []
    assert json.loads(changed.cursor) == {"2024-01": "a2", "2024-02": "b"}
