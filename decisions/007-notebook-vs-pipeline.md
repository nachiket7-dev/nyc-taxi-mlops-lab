# 007: Notebook vs pipeline boundaries

Stage: S2 · Date: 2026-11-21 · Author: Nachiket Amlekar · Drafted by agent: yes

## Choice

Use the SageMaker notebook environment (`notebooks/explore.ipynb`) exclusively for exploratory data analysis, rapid prototyping of model architectures, feature inspection, and error slicing. Keep automated data extraction, feature engineering, model training, evaluation gating, and MLflow alias registration exclusively in deterministic Dagster pipeline assets.

## Rejected alternative

Permitting notebooks to write directly to production feature tables, execute ad-hoc production training runs, or assign the `@challenger` / `@champion` aliases in the MLflow Model Registry.

## Why

Notebooks are interactive, non-linear exploratory scratchpads that lack deterministic state management, automated dependency orchestration, and CI verification. Allowing manual alias assignment or production table manipulation from notebooks introduces human error, configuration drift, and unvetted models into production serving. Dagster pipelines enforce strict schema and feature contracts (`contracts/features.yaml`, `contracts/mlflow.yaml`), automated quality gates, idempotent data processing, and full artifact lineage from Redshift partitions to MLflow model versions.

## What would show this was wrong

If data science iteration speed becomes severely bottlenecked because minor hyperparameter tuning experiments require full pipeline deployments, or conversely, if unvetted notebook code inadvertently modifies production serving state.
