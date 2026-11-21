# S0: security groups and rules.
#
# Decision point: who may reach the app (ingress scope)? Record it in decisions/.
# Traffic between hosts (ports in contracts/runtime.yaml):
#   app -> platform  MLflow (port 5000)
#   app, platform -> llm  chat and embeddings (ports 8080, 8081)
#   app, platform -> rds  Postgres (port 5432)
# There is no SSH in this lab: use SSM Session Manager for a shell and port forwarding for UIs.

# --- App Security Group ---
resource "aws_security_group" "app" {
  name_prefix = "lab-app-"
  vpc_id      = data.aws_vpc.default.id
  description = "App host: port 80 ingress from ingress_cidrs; egress for dependencies"

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_vpc_security_group_ingress_rule" "app_http" {
  for_each          = toset(var.ingress_cidrs)
  security_group_id = aws_security_group.app.id
  description       = "HTTP ingress from allowed CIDR"
  cidr_ipv4         = each.value
  ip_protocol       = "tcp"
  from_port         = 80
  to_port           = 80
}

resource "aws_vpc_security_group_egress_rule" "app_egress_all" {
  security_group_id = aws_security_group.app.id
  description       = "Outbound access for Docker, packages, SSM, and AWS APIs"
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
}

# --- Platform Security Group ---
resource "aws_security_group" "platform" {
  name_prefix = "lab-platform-"
  vpc_id      = data.aws_vpc.default.id
  description = "Platform host: MLflow from app host; egress for dependencies"

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_vpc_security_group_ingress_rule" "platform_mlflow_from_app" {
  security_group_id            = aws_security_group.platform.id
  description                  = "MLflow from app host"
  referenced_security_group_id = aws_security_group.app.id
  ip_protocol                  = "tcp"
  from_port                    = 5000
  to_port                      = 5000
}

resource "aws_vpc_security_group_ingress_rule" "platform_mlflow_from_notebook" {
  security_group_id            = aws_security_group.platform.id
  description                  = "MLflow from SageMaker notebook"
  referenced_security_group_id = module.notebook.security_group_id
  ip_protocol                  = "tcp"
  from_port                    = 5000
  to_port                      = 5000
}

resource "aws_vpc_security_group_egress_rule" "platform_egress_all" {
  security_group_id = aws_security_group.platform.id
  description       = "Outbound access for Docker, packages, SSM, and AWS APIs"
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
}

# --- LLM Security Group ---
resource "aws_security_group" "llm" {
  name_prefix = "lab-llm-"
  vpc_id      = data.aws_vpc.default.id
  description = "LLM host: chat and embeddings from app and platform hosts"

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_vpc_security_group_ingress_rule" "llm_from_app" {
  security_group_id            = aws_security_group.llm.id
  description                  = "LLM endpoints from app host"
  referenced_security_group_id = aws_security_group.app.id
  ip_protocol                  = "tcp"
  from_port                    = 8080
  to_port                      = 8081
}

resource "aws_vpc_security_group_ingress_rule" "llm_from_platform" {
  security_group_id            = aws_security_group.llm.id
  description                  = "LLM endpoints from platform host"
  referenced_security_group_id = aws_security_group.platform.id
  ip_protocol                  = "tcp"
  from_port                    = 8080
  to_port                      = 8081
}

resource "aws_vpc_security_group_egress_rule" "llm_egress_all" {
  security_group_id = aws_security_group.llm.id
  description       = "Outbound access for LLM weights, Docker, and SSM"
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
}

# --- RDS Security Group ---
resource "aws_security_group" "rds" {
  name_prefix = "lab-rds-"
  vpc_id      = data.aws_vpc.default.id
  description = "RDS Postgres: port 5432 from app and platform hosts"

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_vpc_security_group_ingress_rule" "rds_from_app" {
  security_group_id            = aws_security_group.rds.id
  description                  = "Postgres from app host"
  referenced_security_group_id = aws_security_group.app.id
  ip_protocol                  = "tcp"
  from_port                    = 5432
  to_port                      = 5432
}

resource "aws_vpc_security_group_ingress_rule" "rds_from_platform" {
  security_group_id            = aws_security_group.rds.id
  description                  = "Postgres from platform host"
  referenced_security_group_id = aws_security_group.platform.id
  ip_protocol                  = "tcp"
  from_port                    = 5432
  to_port                      = 5432
}

resource "aws_vpc_security_group_egress_rule" "rds_egress_all" {
  security_group_id = aws_security_group.rds.id
  description       = "Outbound access for RDS"
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
}
