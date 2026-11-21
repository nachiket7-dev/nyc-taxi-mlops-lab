# 004: COPY vs Spectrum

Stage: S1 · Date: 2026-11-21 · Author: Nachiket Amlekar · Drafted by agent: yes

## Choice

Ingest monthly trip files directly into Redshift managed storage using the Redshift `COPY` command into `ingest.raw_trips`.

## Rejected alternative

Querying external S3 Parquet files in place using Redshift Spectrum with external schemas and tables.

## Why

COPY loads data into Redshift native columnar storage (`ra3.large`), enabling local column compression, sort keys, zone maps, and fast repeated analytical queries across downstream pipeline stages and marts. In AWS Academy Learner Lab, external schema creation for Spectrum can encounter IAM/AWS Lake Formation permission restrictions, whereas Redshift COPY using `iam_role default` (`LabRole`) is fully supported and reliable. Furthermore, loading into managed tables allows explicit schema enforcement and transaction-isolated idempotent reloads via `delete from ingest.raw_trips where source_file = ...; copy ...`.

## What would show this was wrong

If data volume expands to hundreds of gigabytes exceeding the single-node `ra3.large` managed storage capacity, or if raw ingest COPY durations become prohibitive compared to querying partitioned cold files directly in S3.
