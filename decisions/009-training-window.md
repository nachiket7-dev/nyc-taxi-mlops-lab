# 009: Training window and holdout strategy

Stage: S2 · Date: 2026-11-21 · Author: Nachiket Amlekar · Drafted by agent: yes

## Choice

Train candidate duration models on a 3-month rolling window (`2024-01`, `2024-02`, `2024-03`) and evaluate generalization performance on the subsequent chronological month (`2024-04`) as an out-of-time holdout set.

## Rejected alternative

Performing a random 80/20 train/test split across pooled historical data, or training exclusively on a single recent month.

## Why

Urban transit patterns and traffic dynamics exhibit strong temporal autocorrelation and seasonal drift. Evaluating on a random split causes temporal leakage: trips from the exact same hour or week appear in both training and test sets, artificially inflating validation accuracy. A forward-looking chronological holdout accurately reproduces the production environment where past data must predict future trips. A 3-month window encompasses over 9.5 million trips, providing robust coverage across all 263 zone pairs and diurnal cycles while remaining computationally efficient to train within resource limits.

## What would show this was wrong

If 3 months of history fails to adapt to rapid seasonal shifts (e.g., severe weather transitions) leading to degraded predictions on future months, or if training set volume grows to exceed single-instance memory limits during batch gradient boosting.
