"""S2: training_set, trained_model, eval_report, the challenger gate and the challenger asset.

- training_set: UNLOAD a window of staging.trip_features (and the holdout month) to the artifact bucket.
  The zone-pair distance lookup (contracts/features.yaml, est_distance_miles) comes from the window only.
- trained_model: fit tripmodel.LightGBMModel (the given v2), log it to MLflow as a pyfunc with
  tripmodel.model_signature() and the tripmodel package bundled, register a version of the model named
  in contracts/mlflow.yaml, and tag it as that contract says.
- eval_report: tripmodel.evaluate the new version and the champion (or the v1 baseline before there is
  one) on the same holdout; log the metrics.
- A blocking check that guards a `challenger` asset, which sets the alias. The pipeline never moves
  `champion`.
Decision points: what stays in the notebook; the promotion metric and margin; the training window.
"""

import os
from pathlib import Path

import dagster as dg
import mlflow
from mlflow.exceptions import RestException

import tripmodel
from tripmodel import BaselineModel, LightGBMModel, evaluate, model_signature

from ..contracts import mlflow_contract
from ..resources import ArtifactBucket, MlflowTracking, RedshiftData
from .features import trip_features

TRAINING_WINDOW = ("2024-01", "2024-02", "2024-03")
HOLDOUT_MONTH = "2024-04"
WINDOW_STR = f"{TRAINING_WINDOW[0]}..{TRAINING_WINDOW[-1]}"


@dg.asset(
    deps=[trip_features],
    group_name="model",
    kinds={"redshift", "s3"},
    pool="redshift",
)
def training_set(
    context: dg.AssetExecutionContext,
    redshift: RedshiftData,
    artifacts: ArtifactBucket,
) -> dg.MaterializeResult:
    """UNLOAD window features, holdout features, and training-window distances to S3."""
    train_uri = artifacts.uri("training/train/")
    holdout_uri = artifacts.uri("training/holdout/")
    dist_uri = artifacts.uri("training/distances/")

    # Clean existing training data prefix
    s3_fs = artifacts.filesystem()
    for prefix in ("training/train", "training/holdout", "training/distances"):
        try:
            s3_fs.delete_dir(f"{artifacts.bucket}/{prefix}")
        except Exception:
            pass

    months_in = ", ".join(f"\\\\'{m}\\\\'" for m in TRAINING_WINDOW)
    redshift.execute(
        # 1. Training features (10% sample: ~950k rows for fast, robust training)
        f"UNLOAD ('select pickup_zone_id, dropoff_zone_id, pickup_hour, pickup_dow, duration_min "
        f"from staging.trip_features where trip_month in ({months_in}) and random() < 0.1') "
        f"TO '{train_uri}' IAM_ROLE default FORMAT AS PARQUET ALLOWOVERWRITE PARALLEL OFF",
        # 2. Holdout features (5% sample: ~170k rows for holdout evaluation)
        f"UNLOAD ('select pickup_zone_id, dropoff_zone_id, pickup_hour, pickup_dow, duration_min "
        f"from staging.trip_features where trip_month = \\\\'{HOLDOUT_MONTH}\\\\' and random() < 0.05') "
        f"TO '{holdout_uri}' IAM_ROLE default FORMAT AS PARQUET ALLOWOVERWRITE PARALLEL OFF",
        # 3. Zone-pair distance lookup from the training window only
        f"UNLOAD ('select pickup_zone_id, dropoff_zone_id, median(trip_distance) as est_distance_miles "
        f"from staging.stg_trips where trip_month in ({months_in}) "
        f"and duration_min > 0 and duration_min <= 180 and trip_distance > 0 group by 1, 2') "
        f"TO '{dist_uri}' IAM_ROLE default FORMAT AS PARQUET ALLOWOVERWRITE PARALLEL OFF",
    )

    train_df = artifacts.read_parquet("training/train/")
    holdout_df = artifacts.read_parquet("training/holdout/")
    dist_df = artifacts.read_parquet("training/distances/")

    return dg.MaterializeResult(
        metadata={
            "training_window": dg.MetadataValue.text(WINDOW_STR),
            "holdout_month": dg.MetadataValue.text(HOLDOUT_MONTH),
            "train_rows": dg.MetadataValue.int(len(train_df)),
            "holdout_rows": dg.MetadataValue.int(len(holdout_df)),
            "distance_pairs": dg.MetadataValue.int(len(dist_df)),
        }
    )


@dg.asset(
    deps=[training_set],
    group_name="model",
    kinds={"mlflow"},
)
def trained_model(
    context: dg.AssetExecutionContext,
    artifacts: ArtifactBucket,
    tracking: MlflowTracking,
) -> dg.MaterializeResult:
    """Fit LightGBM v2 model and log/register version in MLflow."""
    train = artifacts.read_parquet("training/train/")
    distances = artifacts.read_parquet("training/distances/")

    for col in ("pickup_zone_id", "dropoff_zone_id", "pickup_hour", "pickup_dow"):
        train[col] = train[col].astype("int64")

    # Fit LightGBMModel
    model = LightGBMModel().fit(train, distances, num_rounds=200)

    contract = mlflow_contract()
    reg_name = contract["registered_model"]

    tracking.client()

    with mlflow.start_run(run_name=f"train_{WINDOW_STR}") as run:
        mlflow.log_params({
            "training_window": WINDOW_STR,
            "holdout_month": HOLDOUT_MONTH,
            "train_rows": len(train),
            "num_rounds": 200,
        })
        info = mlflow.pyfunc.log_model(
            name="model",
            python_model=model,
            signature=model_signature(),
            code_paths=[str(Path(tripmodel.__file__).parent)],
            registered_model_name=reg_name,
        )

    version = info.registered_model_version

    # Store model info for downstream evaluation
    artifacts.write_json("training/latest_model.json", {
        "run_id": run.info.run_id,
        "version": str(version),
        "registered_model": reg_name,
    })

    return dg.MaterializeResult(
        metadata={
            "run_id": dg.MetadataValue.text(run.info.run_id),
            "version": dg.MetadataValue.text(str(version)),
            "registered_model": dg.MetadataValue.text(reg_name),
        }
    )


@dg.asset(
    deps=[trained_model],
    group_name="model",
    kinds={"mlflow"},
)
def eval_report(
    context: dg.AssetExecutionContext,
    artifacts: ArtifactBucket,
    tracking: MlflowTracking,
    redshift: RedshiftData,
) -> dg.MaterializeResult:
    """Evaluate candidate model against champion (or baseline) and log metrics."""
    contract = mlflow_contract()
    reg_name = contract["registered_model"]

    model_info = artifacts.read_json("training/latest_model.json")
    version = model_info["version"]
    run_id = model_info["run_id"]

    holdout = artifacts.read_parquet("training/holdout/")
    for col in ("pickup_zone_id", "dropoff_zone_id", "pickup_hour", "pickup_dow"):
        holdout[col] = holdout[col].astype("int64")

    zones = redshift.query("select location_id, borough from ingest.taxi_zones")
    boroughs = {int(z["location_id"]): z["borough"] for z in zones}

    client = tracking.client()

    # Load and evaluate candidate model
    candidate = mlflow.pyfunc.load_model(f"models:/{reg_name}/{version}")
    metrics = evaluate(candidate.predict, holdout, boroughs, latency_samples=50)

    # Determine champion baseline comparison
    champion_mae = None
    try:
        client.get_model_version_by_alias(reg_name, "champion")
        champ_model = mlflow.pyfunc.load_model(f"models:/{reg_name}@champion")
        champ_eval = evaluate(champ_model.predict, holdout, boroughs, latency_samples=50)
        champion_mae = champ_eval["mae_min"]
    except (RestException, Exception):
        # No champion yet: compare against v1 baseline model
        train = artifacts.read_parquet("training/train/")
        distances = artifacts.read_parquet("training/distances/")
        for col in ("pickup_zone_id", "dropoff_zone_id", "pickup_hour", "pickup_dow"):
            train[col] = train[col].astype("int64")
        v1 = BaselineModel().fit(train, distances)
        v1_eval = evaluate(lambda df: v1.predict(None, df), holdout, boroughs, latency_samples=50)
        champion_mae = v1_eval["mae_min"]

    metrics["champion_mae_min"] = float(champion_mae)

    # Log metrics to MLflow run
    with mlflow.start_run(run_id=run_id):
        mlflow.log_metrics({k: float(v) for k, v in metrics.items() if k != "holdout_rows"})

    # Tag model version as required by contracts/mlflow.yaml
    git_sha = os.environ.get("GIT_SHA", "local")
    tags = {
        "training_window": WINDOW_STR,
        "holdout_month": HOLDOUT_MONTH,
        "features_version": "1",
        "git_sha": git_sha,
        "mae_min": f"{metrics['mae_min']:.4f}",
        "p95_latency_ms": f"{metrics['p95_latency_ms']:.2f}",
        "champion_mae_min": f"{metrics['champion_mae_min']:.4f}",
    }
    for tag_key, tag_val in tags.items():
        client.set_model_version_tag(reg_name, str(version), tag_key, tag_val)

    # Persist report for check & challenger assets
    eval_data = {
        "version": str(version),
        "metrics": metrics,
        "tags": tags,
        "beats_champion": bool(metrics["mae_min"] < champion_mae and metrics["p95_latency_ms"] <= 20.0),
    }
    artifacts.write_json("training/eval_report.json", eval_data)

    return dg.MaterializeResult(
        metadata={
            "version": dg.MetadataValue.text(str(version)),
            "mae_min": dg.MetadataValue.float(metrics["mae_min"]),
            "champion_mae_min": dg.MetadataValue.float(champion_mae),
            "p95_latency_ms": dg.MetadataValue.float(metrics["p95_latency_ms"]),
            "beats_champion": dg.MetadataValue.bool(eval_data["beats_champion"]),
        }
    )


@dg.asset_check(
    asset=eval_report,
    blocking=True,
    description="Candidate must beat champion/baseline MAE and meet p95 latency budget (blocking).",
)
def beats_champion(
    context: dg.AssetCheckExecutionContext,
    artifacts: ArtifactBucket,
) -> dg.AssetCheckResult:
    """Guards the challenger alias: candidate must beat the champion/baseline within latency budget."""
    eval_data = artifacts.read_json("training/eval_report.json")
    metrics = eval_data["metrics"]
    mae = metrics["mae_min"]
    champ_mae = metrics["champion_mae_min"]
    p95 = metrics["p95_latency_ms"]

    passed = mae < champ_mae and p95 <= 20.0
    desc = f"Candidate MAE {mae:.2f} vs champion/baseline {champ_mae:.2f}; p95 latency {p95:.1f}ms (budget 20ms)"

    return dg.AssetCheckResult(
        passed=passed,
        severity=dg.AssetCheckSeverity.ERROR,
        description=desc,
        metadata={
            "mae_min": dg.MetadataValue.float(mae),
            "champion_mae_min": dg.MetadataValue.float(champ_mae),
            "p95_latency_ms": dg.MetadataValue.float(p95),
        },
    )


@dg.asset(
    deps=[eval_report],
    group_name="model",
    kinds={"mlflow"},
)
def challenger(
    context: dg.AssetExecutionContext,
    artifacts: ArtifactBucket,
    tracking: MlflowTracking,
) -> dg.MaterializeResult:
    """Set the challenger alias in MLflow for the validated model version."""
    contract = mlflow_contract()
    reg_name = contract["registered_model"]
    eval_data = artifacts.read_json("training/eval_report.json")
    version = eval_data["version"]

    client = tracking.client()
    client.set_registered_model_alias(reg_name, "challenger", version)

    return dg.MaterializeResult(
        metadata={
            "registered_model": dg.MetadataValue.text(reg_name),
            "version": dg.MetadataValue.text(str(version)),
            "alias": dg.MetadataValue.text("challenger"),
        }
    )


train_model = dg.define_asset_job(
    "train_model",
    selection=dg.AssetSelection.assets(training_set, trained_model, eval_report, challenger),
)
