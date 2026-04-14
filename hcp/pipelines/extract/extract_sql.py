"""
extract_sql.py
Pulls raw claims data from a SQL database using SQLAlchemy.
Supports PostgreSQL, SQL Server, and Snowflake connection strings.
"""

import pandas as pd
import logging
from sqlalchemy import create_engine, text
import yaml
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_engine(config_path: str = "config/config.yaml"):
    """Build a SQLAlchemy engine from config file."""
    with open(config_path) as f:
        cfg = yaml.safe_load(f)

    db = cfg["sources"]["sql_db"]
    conn_str = (
        f"{db['dialect']}://{db['user']}:{db['password']}"
        f"@{db['host']}:{db['port']}/{db['database']}"
    )
    logger.info(f"Connecting to: {db['host']} / {db['database']}")
    return create_engine(conn_str)


def extract_claims(config_path: str = "config/config.yaml") -> pd.DataFrame:
    """Extract all claims records from the source database."""
    engine = get_engine(config_path)

    query = text("""
        SELECT
            claim_id,
            member_id,
            provider_id,
            claim_date,
            service_date,
            claim_type,
            diagnosis_code,
            procedure_code,
            billed_amount,
            allowed_amount,
            paid_amount,
            claim_status,
            denial_reason,
            processing_days,
            plan_type,
            region
        FROM claims
        WHERE claim_date >= DATEADD(year, -2, GETDATE())
    """)

    logger.info("Extracting claims from database...")
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)

    logger.info(f"Extracted {len(df):,} claim records")
    return df


def extract_members(config_path: str = "config/config.yaml") -> pd.DataFrame:
    """Extract member reference data."""
    engine = get_engine(config_path)

    query = text("""
        SELECT member_id, member_name, date_of_birth, gender, state, plan_type
        FROM members
    """)

    with engine.connect() as conn:
        df = pd.read_sql(query, conn)

    logger.info(f"Extracted {len(df):,} member records")
    return df


if __name__ == "__main__":
    df = extract_claims()
    print(df.head())
    print(df.dtypes)
