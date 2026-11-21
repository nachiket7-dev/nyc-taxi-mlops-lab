# 008: Promotion metric and margin

Stage: S2 · Date: 2026-11-21 · Author: Nachiket Amlekar · Drafted by agent: yes

## Choice

Use Mean Absolute Error (`mae_min`) on the chronological holdout month (`2024-04`) as the primary promotion metric. Require candidate models to achieve a lower `mae_min` than the active `@champion` (or the v1 baseline when no champion exists) while strictly adhering to a single-row inference latency budget of `p95_latency_ms <= 20.0` ms.

## Rejected alternative

Using Root Mean Squared Error (RMSE) as the loss/promotion criterion, or promoting models on aggregate accuracy alone without an enforced latency SLA.

## Why

Trip durations in urban transportation display severe right-skew with heavy tails driven by traffic anomalies, accidents, and meter errors. RMSE squares residuals, disproportionately penalizing large outliers and biasing predictions upward for typical commuters. MAE minimizes median expected error, rewarding accuracy on everyday trips while remaining robust against extreme sensor noise. Furthermore, serving models under real-time application constraints requires low-latency execution; an accurate model that exceeds the latency budget cannot be served to live API callers.

## What would show this was wrong

If a model with superior overall MAE exhibits catastrophic error spikes on critical traffic slices (e.g., airport or night trips) that harm customer trust, or if serving infrastructure suffers severe p99 latency degradation under high concurrency despite passing the p95 offline check.
