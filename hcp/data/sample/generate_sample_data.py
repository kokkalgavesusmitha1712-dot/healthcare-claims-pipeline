"""
generate_sample_data.py
Generates realistic sample healthcare claims data for testing the pipeline.
Run this once to create data/sample/claims_sample.csv
"""

import pandas as pd
import numpy as np
import random
from pathlib import Path

random.seed(42)
np.random.seed(42)

N = 500

claim_types    = ["medical", "pharmacy", "dental", "vision", "behavioral"]
statuses       = ["approved", "denied", "pending", "under_review"]
status_weights = [0.70, 0.18, 0.08, 0.04]
denial_reasons = ["not covered", "prior auth required", "duplicate claim",
                  "out of network", "exceeded benefit limit", "missing documentation"]
regions        = ["NE", "SE", "MW", "SW", "W"]
plan_types     = ["PPO", "HMO", "EPO", "HDHP"]
diag_codes     = ["Z00.00","I10","E11.9","J06.9","M54.5","K21.0","F32.9"]
proc_codes     = ["99213","99214","87880","93000","71046","80053","97110"]

dates = pd.date_range("2023-01-01", "2024-12-31", periods=N)

df = pd.DataFrame({
    "claim_id":        [f"CLM{str(i).zfill(5)}" for i in range(1, N + 1)],
    "member_id":       [f"MBR{random.randint(1000,9999)}" for _ in range(N)],
    "provider_id":     [f"PRV{random.randint(100,999)}"  for _ in range(N)],
    "claim_date":      dates.strftime("%Y-%m-%d"),
    "service_date":    (dates - pd.to_timedelta(np.random.randint(1, 5, N), unit="D")).strftime("%Y-%m-%d"),
    "claim_type":      np.random.choice(claim_types, N),
    "diagnosis_code":  np.random.choice(diag_codes, N),
    "procedure_code":  np.random.choice(proc_codes, N),
    "billed_amount":   np.round(np.random.uniform(50, 5000, N), 2),
    "allowed_amount":  np.round(np.random.uniform(40, 4500, N), 2),
    "paid_amount":     np.round(np.random.uniform(30, 4000, N), 2),
    "claim_status":    np.random.choice(statuses, N, p=status_weights),
    "processing_days": np.random.randint(1, 20, N),
    "region":          np.random.choice(regions, N),
    "plan_type":       np.random.choice(plan_types, N),
})

df.loc[df["claim_status"] == "denied", "denial_reason"] = np.random.choice(
    denial_reasons, (df["claim_status"] == "denied").sum()
)

output = Path("data/sample/claims_sample.csv")
output.parent.mkdir(parents=True, exist_ok=True)
df.to_csv(output, index=False)
print(f"Generated {N} sample claims → {output}")
print(df.head())
