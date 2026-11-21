# 003: Ingress scope

Stage: S0 · Date: 2026-11-21 · Author: Nachiket Amlekar · Drafted by agent: yes

## Choice

Restrict inbound traffic on port 80 of the app host to explicit CIDR blocks (`var.ingress_cidrs`), covering the student's workstation IP and any grader/evaluator IP ranges.

## Rejected alternative

Opening port 80 to `0.0.0.0/0` (the entire public internet).

## Why

The app host exposes an unauthenticated HTTP service running on Traefik without a Web Application Firewall (WAF), rate limiting, or TLS termination. Opening port 80 to `0.0.0.0/0` invites internet-wide automated crawlers, port scanners, and potential denial-of-service traffic that could consume compute, memory, and Learner Lab budget. Scoping ingress to trusted CIDRs limits exposure exclusively to authorized evaluators and the operator.

## What would show this was wrong

If the operator's public IP changes frequently (e.g., dynamic residential IP or mobile tethering) leading to intermittent access denials until Terraform updates are applied, or if grader IPs are unannounced and unable to reach the endpoint.
