# S1: wire the Redshift cluster (infra/modules/redshift) into this root module, and expose the
# outputs that contracts/platform.yaml lists under terraform_outputs.

module "redshift" {
  source     = "../modules/redshift"
  subnet_ids = data.aws_subnets.default.ids
}
