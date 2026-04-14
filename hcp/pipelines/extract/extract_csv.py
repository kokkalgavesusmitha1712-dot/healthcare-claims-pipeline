"""
extract_csv.py
Ingests raw claims data from CSV / Excel flat files into a Pandas DataFrame.
"""

import pandas as pd
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_csv(filepath: str) -> pd.DataFrame:
    """Load a CSV claims file and return a raw DataFrame."""
    path = Path(filepath)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {filepath}")

    logger.info(f"Loading CSV file: {filepath}")
    df = pd.read_csv(filepath, dtype=str)  # read all as str to avoid type coercion
    logger.info(f"Loaded {len(df):,} rows and {len(df.columns)} columns")
    return df


def load_excel(filepath: str, sheet_name: str = 0) -> pd.DataFrame:
    """Load an Excel claims file and return a raw DataFrame."""
    path = Path(filepath)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {filepath}")

    logger.info(f"Loading Excel file: {filepath}, sheet: {sheet_name}")
    df = pd.read_excel(filepath, sheet_name=sheet_name, dtype=str)
    logger.info(f"Loaded {len(df):,} rows and {len(df.columns)} columns")
    return df


def load_all_csvs(directory: str) -> pd.DataFrame:
    """Load and concatenate all CSV files in a directory."""
    folder = Path(directory)
    files = list(folder.glob("*.csv"))
    if not files:
        raise ValueError(f"No CSV files found in: {directory}")

    logger.info(f"Found {len(files)} CSV files in {directory}")
    frames = [load_csv(str(f)) for f in files]
    combined = pd.concat(frames, ignore_index=True)
    logger.info(f"Combined total: {len(combined):,} rows")
    return combined


if __name__ == "__main__":
    df = load_csv("data/sample/claims_sample.csv")
    print(df.head())
