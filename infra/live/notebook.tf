# S2: wire infra/modules/notebook (the SageMaker notebook) into this root module, let it reach MLflow
# on the platform host (port 5000, runtime.yaml), and output its name as notebook_name.
# Set github_repo in terraform.tfvars: the notebook clones your public repo.

module "notebook" {
  source    = "../modules/notebook"
  subnet_id = data.aws_subnet.selected.id
  repo_url  = "https://github.com/${var.github_repo}.git"

  env = {
    MLFLOW_TRACKING_URI = "http://${module.host_platform.private_ip}:5000"
    REDSHIFT_CLUSTER_ID = module.redshift.cluster_id
    REDSHIFT_DATABASE   = module.redshift.database
    LAB_ARTIFACT_BUCKET = module.artifact_bucket.name
    LAB_DATA_BUCKET     = "nst-mlops-2026"
  }
}

output "notebook_name" {
  value = module.notebook.name
}
