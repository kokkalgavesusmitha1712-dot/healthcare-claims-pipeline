"""
kpi_engine.py
Calculates all business KPIs from cleaned claims data.

KPIs produced:
  - Claim approval rate
  - Claim denial rate
  - Average processing time (days)
  - Average cost per claim
  - Total billed vs paid variance
  - Denial trends by reason
  - Monthly claim volume
  - KPIs by region and plan type
"""

import pandas as pd
import numpy as np
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def kpi_approval_denial_rate(df: pd.DataFrame) -> pd.DataFrame:
    """Overall approval and denial rates."""
    total    = len(df)
    approved = df["is_approved"].sum()
    denied   = df["is_denied"].sum()
    pending  = total - approved - denied

    result = pd.DataFrame([{
        "total_claims":    total,
        "approved_claims": int(approved),
        "denied_claims":   int(denied),
        "pending_claims":  int(pending),
        "approval_rate_pct": round(approved / total * 100, 2) if total else 0,
        "denial_rate_pct":   round(denied  / total * 100, 2) if total else 0,
    }])
    logger.info(f"Approval rate: {result['approval_rate_pct'].iloc[0]}%")
    return result


def kpi_processing_time(df: pd.DataFrame) -> pd.DataFrame:
    """Average, median, and max processing time in days."""
    pt = df["processing_days"].dropna()
    result = pd.DataFrame([{
        "avg_processing_days":    round(pt.mean(), 1),
        "median_processing_days": round(pt.median(), 1),
        "max_processing_days":    int(pt.max()),
        "pct_under_5_days":       round((pt <= 5).sum() / len(pt) * 100, 1) if len(pt) else 0,
    }])
    logger.info(f"Avg processing time: {result['avg_processing_days'].iloc[0]} days")
    return result


def kpi_cost_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Total billed, paid, allowed and variance metrics."""
    result = pd.DataFrame([{
        "total_billed_amount":   round(df["billed_amount"].sum(), 2),
        "total_paid_amount":     round(df["paid_amount"].sum(), 2),
        "total_allowed_amount":  round(df["allowed_amount"].sum(), 2),
        "total_cost_variance":   round(df["cost_variance"].sum(), 2),
        "avg_billed_per_claim":  round(df["billed_amount"].mean(), 2),
        "avg_paid_per_claim":    round(df["paid_amount"].mean(), 2),
    }])
    logger.info(f"Total billed: ${result['total_billed_amount'].iloc[0]:,.2f}")
    return result


def kpi_denial_by_reason(df: pd.DataFrame) -> pd.DataFrame:
    """Claim denial counts and rate broken down by denial reason."""
    denied = df[df["is_denied"] & df["denial_reason"].notna() & (df["denial_reason"] != "N/A")]
    if denied.empty:
        return pd.DataFrame(columns=["denial_reason", "count", "pct_of_denials"])

    breakdown = (
        denied.groupby("denial_reason")
        .size()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
    )
    breakdown["pct_of_denials"] = round(breakdown["count"] / len(denied) * 100, 1)
    return breakdown


def kpi_monthly_volume(df: pd.DataFrame) -> pd.DataFrame:
    """Monthly claim volume, approval rate, and avg cost."""
    monthly = (
        df.groupby("claim_year_month")
        .agg(
            total_claims   =("claim_id",       "count"),
            approved_claims=("is_approved",    "sum"),
            denied_claims  =("is_denied",      "sum"),
            total_billed   =("billed_amount",  "sum"),
            total_paid     =("paid_amount",    "sum"),
        )
        .reset_index()
    )
    monthly["approval_rate_pct"] = round(monthly["approved_claims"] / monthly["total_claims"] * 100, 1)
    monthly["avg_cost_per_claim"] = round(monthly["total_billed"] / monthly["total_claims"], 2)
    monthly = monthly.sort_values("claim_year_month")
    return monthly


def kpi_by_region(df: pd.DataFrame) -> pd.DataFrame:
    """KPIs grouped by region."""
    if "region" not in df.columns:
        return pd.DataFrame()

    region = (
        df.groupby("region")
        .agg(
            total_claims   =("claim_id",      "count"),
            approved_claims=("is_approved",   "sum"),
            total_billed   =("billed_amount", "sum"),
            total_paid     =("paid_amount",   "sum"),
        )
        .reset_index()
    )
    region["approval_rate_pct"] = round(region["approved_claims"] / region["total_claims"] * 100, 1)
    return region.sort_values("total_claims", ascending=False)


def kpi_by_plan_type(df: pd.DataFrame) -> pd.DataFrame:
    """KPIs grouped by plan type."""
    if "plan_type" not in df.columns:
        return pd.DataFrame()

    plan = (
        df.groupby("plan_type")
        .agg(
            total_claims   =("claim_id",      "count"),
            approved_claims=("is_approved",   "sum"),
            denied_claims  =("is_denied",     "sum"),
            total_paid     =("paid_amount",   "sum"),
        )
        .reset_index()
    )
    plan["approval_rate_pct"] = round(plan["approved_claims"] / plan["total_claims"] * 100, 1)
    return plan


def run_all_kpis(df: pd.DataFrame) -> dict:
    """Run all KPI functions and return results as a dict of DataFrames."""
    logger.info("Running KPI engine...")
    kpis = {
        "approval_denial_rate": kpi_approval_denial_rate(df),
        "processing_time":      kpi_processing_time(df),
        "cost_summary":         kpi_cost_summary(df),
        "denial_by_reason":     kpi_denial_by_reason(df),
        "monthly_volume":       kpi_monthly_volume(df),
        "by_region":            kpi_by_region(df),
        "by_plan_type":         kpi_by_plan_type(df),
    }
    logger.info("KPI engine complete")
    return kpis


if __name__ == "__main__":
    import sys
    sys.path.append(".")
    from pipelines.transform.clean import clean_claims

    sample = pd.DataFrame({
        "claim_id":        ["C001","C002","C003","C004","C005"],
        "member_id":       ["M1","M2","M3","M4","M5"],
        "provider_id":     ["P1","P2","P3","P1","P2"],
        "claim_date":      ["2024-01-10","2024-01-20","2024-02-05","2024-02-15","2024-03-01"],
        "service_date":    ["2024-01-08","2024-01-18","2024-02-03","2024-02-13","2024-02-28"],
        "claim_type":      ["medical","pharmacy","dental","medical","pharmacy"],
        "billed_amount":   [1500, 200, 350, 900, 120],
        "allowed_amount":  [1200, 180, 300, 800, 100],
        "paid_amount":     [1100, 160, 280, 750, 90],
        "claim_status":    ["approved","denied","approved","denied","approved"],
        "denial_reason":   [None,"not covered",None,"prior auth required",None],
        "processing_days": [5, 3, 7, 4, 2],
        "region":          ["NE","SE","MW","NE","SW"],
        "plan_type":       ["PPO","HMO","PPO","EPO","HMO"],
    })

    cleaned = clean_claims(sample)
    results = run_all_kpis(cleaned)
    for name, df_kpi in results.items():
        print(f"\n--- {name} ---")
        print(df_kpi)
