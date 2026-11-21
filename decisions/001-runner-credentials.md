# 001: Runner credentials

Stage: S0 · Date: 2026-11-21 · Author: Nachiket Amlekar · Drafted by agent: yes

## Choice

Use the EC2 instance profile (`LabInstanceProfile`) on a self-hosted runner running on the app host, rather than storing static AWS session credentials in GitHub Secrets.

## Rejected alternative

Storing temporary Learner Lab session credentials (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`) as GitHub Actions repository secrets and running deployments from GitHub-hosted runners.

## Why

Learner Lab session credentials expire every few hours, requiring frequent manual exports via `scripts/gh-secrets.sh` and causing CD pipeline failures whenever a session refreshes or expires. By running a self-hosted runner on the host, the deployment job automatically uses `LabInstanceProfile` through the Instance Metadata Service (IMDSv2), which remains valid across host reboots and session restarts without manual secret rotations. Furthermore, keeping long-lived or session cloud credentials out of GitHub Actions secrets eliminates exfiltration risks in a public repository.

## What would show this was wrong

If maintaining self-hosted runners creates unacceptable operational overhead, or if unreviewed pull requests execute arbitrary code on the self-hosted runner and compromise the host or `LabRole` (mitigated by requiring approval for all external contributors on fork PRs and ensuring self-hosted jobs never run on `pull_request` triggers).
