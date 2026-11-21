# 002: Image tag scheme

Stage: S0 · Date: 2026-11-21 · Author: Nachiket Amlekar · Drafted by agent: yes

## Choice

Use the git commit SHA prefix (`sha-<short-sha>`, e.g. `sha-${GITHUB_SHA::7}`) as the immutable identifier for application container images.

## Rejected alternative

Using mutable tags such as `latest`, build timestamps, or manual semantic versioning tags (`vX.Y.Z`) on each push.

## Why

In continuous deployment from `main`, every commit represents an immutable release artifact. Tagging images with their git commit SHA provides exact 1:1 traceability from the running container (`/version` returns `git_sha`) back to the specific source tree state and commit history. Rollbacks can be performed deterministically by referencing the exact prior commit SHA without needing to re-tag or rebuild images. Mutable tags like `latest` prevent reproducible rollbacks and risk caching stale layers across Docker hosts.

## What would show this was wrong

If external consumers or release processes require human-meaningful semantic version numbers, or if comparing build recency without querying git metadata becomes a frequent operational requirement.
