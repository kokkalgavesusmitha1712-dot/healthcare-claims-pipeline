"""
clean.py
Cleans and standardises raw claims data:
  - Type casting
  - Null handling
  - Deduplication
  - Value normalisation
"""

import pandas as pd
import numpy as np
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

REQUIRED_COLUMNS = [
    "claim_id", "member_id", "provider_id", "claim_date",
    "claim_type", "billed_amount", "paid_amount", "claim_status"
]

VALID_CLAIM_STATUSES = {"approved", "denied", "pending", "under_review"}
VALID_CLAIM_TYPES    = {"medical", "pharmacy", "dental", "vision", "behavioral"}


def validate_schema(df: pd.DataFrame) -> None:
    """Raise if any required columns are missing."""
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    logger.info("Schema validation passed")


def cast_types(df: pd.DataFrame) -> pd.DataFrame:
    """Cast columns to their correct data types."""
    df = df.copy()

    date_cols = ["claim_date", "service_date"]
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    money_cols = ["billed_amount", "allowed_amount", "paid_amount"]
    for col in money_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "processing_days" in df.columns:
        df["processing_days"] = pd.to_numeric(df["processing_days"], errors="coerce")

    logger.info("Type casting complete")
    return df


def handle_nulls(df: pd.DataFrame) -> pd.DataFrame:
    """Handle null values with domain-appropriate defaults."""
    df = df.copy()

    df["claim_status"]   = df["claim_status"].fillna("pending")
    df["denial_reason"]  = df["denial_reason"].fillna("N/A") if "denial_reason" in df.columns else df.get("denial_reason", "N/A")
    df["allowed_amount"] = df["allowed_amount"].fillna(0.0)
    df["paid_amount"]    = df["paid_amount"].fillna(0.0)

    null_counts = df[REQUIRED_COLUMNS].isnull().sum()
    if null_counts.any():
        logger.warning(f"Remaining nulls in required columns:\n{null_counts[null_counts > 0]}")

    return df


def deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    """Remove duplicate claim records, keeping the latest."""
    before = len(df)
    df = df.sort_values("claim_date", ascending=False)
    df = df.drop_duplicates(subset=["claim_id"], keep="first")
    removed = before - len(df)
    if removed:
        logger.warning(f"Removed {removed:,} duplicate claim_ids")
    logger.info(f"After deduplication: {len(df):,} records")
    return df


def normalise_values(df: pd.DataFrame) -> pd.DataFrame:
    """Standardise categorical values to lowercase."""
    df = df.copy()

    df["claim_status"] = df["claim_status"].str.strip().str.lower()
    df["claim_type"]   = df["claim_type"].str.strip().str.lower() if "claim_type" in df.columns else df.get("claim_type")

    invalid_status = ~df["claim_status"].isin(VALID_CLAIM_STATUSES)
    if invalid_status.any():
        logger.warning(f"{invalid_status.sum()} records have unrecognised claim_status — setting to 'pending'")
        df.loc[invalid_status, "claim_status"] = "pending"

    return df


def add_derived_fields(df: pd.DataFrame) -> pd.DataFrame:
    """Add calculated helper columns used by the KPI engine."""
    df = df.copy()
    df["is_denied"]        = df["claim_status"] == "denied"
    df["is_approved"]      = df["claim_status"] == "approved"
    df["claim_year_month"] = df["claim_date"].dt.to_period("M").astype(str)
    df["cost_variance"]    = df["billed_amount"] - df["paid_amount"]
    return df


def clean_claims(df: pd.DataFrame) -> pd.DataFrame:
    """Master cleaning function — runs full pipeline."""
    logger.info(f"Starting clean — input rows: {len(df):,}")
    validate_schema(df)
    df = cast_types(df)
    df = handle_nulls(df)
    df = deduplicate(df)
    df = normalise_values(df)
    df = add_derived_fields(df)
    logger.info(f"Cleaning complete — output rows: {len(df):,}")
    return df


if __name__ == "__main__":
    sample = pd.DataFrame({
        "claim_id":       ["C001", "C002", "C001", "C003"],
        "member_id":      ["M1", "M2", "M1", "M3"],
        "provider_id":    ["P1", "P2", "P1", "P3"],
        "claim_date":     ["2024-01-15", "2024-02-10", "2024-01-15", None],
        "claim_type":     ["Medical", "pharmacy", "Medical", "DENTAL"],
        "billed_amount":  ["1500.00", "200.50", "1500.00", "350"],
        "allowed_amount": ["1200.00", None, "1200.00", "300"],
        "paid_amount":    ["1100.00", "180.00", "1100.00", "290"],
        "claim_status":   ["Approved", "denied", "Approved", None],
        "denial_reason":  [None, "not covered", None, None],
        "processing_days":["5", "3", "5", "7"],
    })

    cleaned = clean_claims(sample)
    print(cleaned)
