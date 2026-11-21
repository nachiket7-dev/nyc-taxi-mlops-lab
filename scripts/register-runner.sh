#!/usr/bin/env bash
# S0: register a lab host as a self-hosted GitHub Actions runner (label: lab-<role>, from
# contracts/runtime.yaml), so the CD workflow can deploy with the host's instance profile.
#
# Usage:
#   scripts/register-runner.sh [app|platform|llm]
#
# Decision point: instance profile vs static credentials for the runner. Record it in decisions/.
# Constraints: no SSH in this lab (use SSM), never commit or print tokens. The repo is public, so
# set fork-PR workflow approval to all external contributors before the runner exists.
set -euo pipefail
export AWS_REGION=us-east-1 AWS_DEFAULT_REGION=us-east-1 AWS_PAGER=""

role="${1:-app}"
label="lab-${role}"
root="$(cd "$(dirname "$0")/.." && pwd)"

# Verify prerequisites
command -v aws >/dev/null || { echo "aws CLI missing" >&2; exit 1; }
command -v gh >/dev/null || { echo "gh CLI missing" >&2; exit 1; }
command -v jq >/dev/null || { echo "jq missing" >&2; exit 1; }

repo=$(gh repo view --json nameWithOwner -q .nameWithOwner 2>/dev/null || git remote get-url origin 2>/dev/null | sed -E 's/.*[:\/]([^\/]+\/[^\/]+)(\.git)?$/\1/' | sed 's/\.git$//')
echo "== Registering runner for role: ${role} (${label}) on repo: ${repo}"

# 1. Look up instance ID
instance_id=$(terraform -chdir="$root/infra/live" output -raw "${role}_instance_id" 2>/dev/null || true)
if [[ -z "${instance_id:-}" || "${instance_id}" == "No outputs found"* ]]; then
  instance_id=$(aws ec2 describe-instances \
    --filters "Name=tag:Name,Values=lab-${role}" "Name=instance-state-name,Values=running" \
    --query "Reservations[0].Instances[0].InstanceId" --output text 2>/dev/null || true)
fi

if [[ -z "${instance_id:-}" || "${instance_id}" == "None" ]]; then
  echo "Error: Could not find running EC2 instance for lab-${role}" >&2
  exit 1
fi
echo "Found instance ${instance_id} for lab-${role}"

# 2. Wait for SSM agent to be online
echo "Waiting for SSM agent to be online on ${instance_id}..."
for _ in $(seq 1 30); do
  status=$(aws ssm describe-instance-information \
    --filters "Key=InstanceIds,Values=${instance_id}" \
    --query "InstanceInformationList[0].PingStatus" --output text 2>/dev/null || echo "")
  if [[ "$status" == "Online" ]]; then
    break
  fi
  sleep 5
done

# 3. Wait for host bootstrap to complete (/var/lib/lab-setup-done)
echo "Waiting for user-data bootstrap script to finish..."
wait_script='while [ ! -f /var/lib/lab-setup-done ]; do sleep 2; done'
cmd_id=$(aws ssm send-command \
  --instance-ids "${instance_id}" \
  --document-name "AWS-RunShellScript" \
  --parameters "{\"commands\":[$(jq -Rs . <<<"$wait_script")]}" \
  --query "Command.CommandId" --output text)
aws ssm wait command-executed --command-id "${cmd_id}" --instance-id "${instance_id}"

# 4. Obtain registration token from GitHub API (never printed)
token=$(gh api -X POST "repos/${repo}/actions/runners/registration-token" --jq .token)
[[ -n "$token" ]] || { echo "Failed to get runner registration token" >&2; exit 1; }

RUNNER_VERSION="${RUNNER_VERSION:-2.338.2}"

# 5. Configure and run GitHub Actions runner as systemd service
echo "Configuring GitHub Actions runner on host..."
setup_script=$(cat <<EOF
set -euo pipefail
cd /opt/actions-runner

# Remove old runner files if version changed or corrupted
if [ -f ./bin/Runner.Listener ]; then
  current_ver=\$(./bin/Runner.Listener --version 2>/dev/null || echo "0.0.0")
  if [ "\$current_ver" != "${RUNNER_VERSION}" ]; then
    ./svc.sh stop 2>/dev/null || true
    ./svc.sh uninstall 2>/dev/null || true
    rm -rf *
  fi
fi

if [ ! -f ./config.sh ]; then
  curl -fsSL -o actions-runner.tar.gz "https://github.com/actions/runner/releases/download/v${RUNNER_VERSION}/actions-runner-linux-arm64-${RUNNER_VERSION}.tar.gz"
  tar xzf actions-runner.tar.gz
  rm -f actions-runner.tar.gz
  chown -R runner:runner /opt/actions-runner
fi

./svc.sh stop 2>/dev/null || true
./svc.sh uninstall 2>/dev/null || true

sudo -u runner ./config.sh \
  --url "https://github.com/${repo}" \
  --token "${token}" \
  --labels "${label}" \
  --name "lab-${role}" \
  --unattended \
  --replace

./svc.sh install runner
./svc.sh start
EOF
)

cmd_id=$(aws ssm send-command \
  --instance-ids "${instance_id}" \
  --document-name "AWS-RunShellScript" \
  --parameters "{\"commands\":[$(jq -Rs . <<<"$setup_script")]}" \
  --query "Command.CommandId" --output text)
aws ssm wait command-executed --command-id "${cmd_id}" --instance-id "${instance_id}"

echo "== Runner ${label} registered and active on ${repo}!"
