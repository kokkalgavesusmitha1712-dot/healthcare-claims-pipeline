"""
load_warehouse.py
Loads cleaned claims and KPI results into the target data warehouse.
Supports PostgreSQL, Snowflake, and local Parquet/CSV export.
"""

import pandas as pd
import logging
import yaml
import os
from pathlib import Path
from sqlalchemy import create_engine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_engine(config_path: str = "config/config.yaml"):
    """Build a SQLAlchemy engine from config."""
    with open(config_path) as f:
        cfg = yaml.safe_load(f)

    db = cfg["warehouse"]
    conn_str = (
        f"{db['dialect']}://{db['user']}:{db['password']}"
        f"@{db['host']}:{db['port']}/{db['database']}"
    )
    logger.info(f"Connecting to warehouse: {db['host']} / {db['database']}")
    return create_engine(conn_str)


def load_to_sql(df: pd.DataFrame, table_name: str,
                config_path: str = "config/config.yaml",
                if_exists: str = "append") -> None:
    """Load a DataFrame into a SQL table."""
    engine = get_engine(config_path)
    logger.info(f"Loading {len(df):,} rows → {table_name} (if_exists='{if_exists}')")
    df.to_sql(table_name, engine, if_exists=if_exists, index=False, chunksize=1000)
    logger.info(f"Load complete: {table_name}")


def load_kpis_to_sql(kpi_dict: dict, config_path: str = "config/config.yaml") -> None:
    """Load all KPI DataFrames into corresponding warehouse tables."""
    table_map = {
        "approval_denial_rate": "kpi_approval_denial_rate",
        "processing_time":      "kpi_processing_time",
        "cost_summary":         "kpi_cost_summary",
        "denial_by_reason":     "kpi_denial_by_reason",
        "monthly_volume":       "kpi_monthly_volume",
        "by_region":            "kpi_by_region",
        "by_plan_type":         "kpi_by_plan_type",
    }
    for kpi_name, df in kpi_dict.items():
        if df is not None and not df.empty:
            table = table_map.get(kpi_name, f"kpi_{kpi_name}")
            load_to_sql(df, table, config_path, if_exists="replace")


def export_to_parquet(df: pd.DataFrame, output_path: str) -> None:
    """Export a DataFrame to a Parquet file for downstream use."""
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)
    logger.info(f"Exported {len(df):,} rows to {output_path}")


def export_to_csv(df: pd.DataFrame, output_path: str) -> None:
    """Export a DataFrame to CSV (useful for Power BI / Tableau direct connect)."""
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    logger.info(f"Exported {len(df):,} rows to {output_path}")


def export_all_kpis_csv(kpi_dict: dict, output_dir: str = "data/processed") -> None:
    """Export all KPI DataFrames to CSV files in a directory."""
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    for kpi_name, df in kpi_dict.items():
        if df is not None and not df.empty:
            path = f"{output_dir}/{kpi_name}.csv"
            export_to_csv(df, path)
    logger.info(f"All KPI CSVs exported to {output_dir}/")


if __name__ == "__main__":
    import sys
    sys.path.append(".")
    from pipelines.transform.clean import clean_claims
    from pipelines.transform.kpi_engine import run_all_kpis

    sample = pd.DataFrame({
        "claim_id":        ["C001","C002","C003"],
        "member_id":       ["M1","M2","M3"],
        "provider_id":     ["P1","P2","P3"],
        "claim_date":      ["2024-01-10","2024-02-05","2024-03-01"],
        "service_date":    ["2024-01-08","2024-02-03","2024-02-28"],
        "claim_type":      ["medical","pharmacy","dental"],
        "billed_amount":   [1500, 200, 350],
        "allowed_amount":  [1200, 180, 300],
        "paid_amount":     [1100, 160, 280],
        "claim_status":    ["approved","denied","approved"],
        "denial_reason":   [None,"not covered",None],
        "processing_days": [5, 3, 7],
        "region":          ["NE","SE","MW"],
        "plan_type":       ["PPO","HMO","PPO"],
    })

    cleaned = clean_claims(sample)
    kpis    = run_all_kpis(cleaned)

    export_to_csv(cleaned, "data/processed/claims_cleaned.csv")
    export_all_kpis_csv(kpis)
    print("Export complete — check data/processed/")
