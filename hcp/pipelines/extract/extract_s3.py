"""
extract_s3.py
Downloads claims data files from AWS S3 and returns a DataFrame.
Requires: boto3, pandas, pyarrow (for parquet support)
"""

import boto3
import pandas as pd
import logging
import io
import yaml

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_s3_client(config_path: str = "config/config.yaml"):
    """Create an S3 client from config."""
    with open(config_path) as f:
        cfg = yaml.safe_load(f)

    aws = cfg["sources"]["aws_s3"]
    return boto3.client(
        "s3",
        region_name=aws["region"],
        aws_access_key_id=aws.get("access_key_id"),
        aws_secret_access_key=aws.get("secret_access_key"),
    )


def list_s3_files(prefix: str, config_path: str = "config/config.yaml") -> list:
    """List all files under a given S3 prefix."""
    with open(config_path) as f:
        cfg = yaml.safe_load(f)

    bucket = cfg["sources"]["aws_s3"]["bucket"]
    client = get_s3_client(config_path)

    response = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    files = [obj["Key"] for obj in response.get("Contents", [])]
    logger.info(f"Found {len(files)} files under s3://{bucket}/{prefix}")
    return files


def extract_s3_csv(s3_key: str, config_path: str = "config/config.yaml") -> pd.DataFrame:
    """Download a CSV file from S3 and return as DataFrame."""
    with open(config_path) as f:
        cfg = yaml.safe_load(f)

    bucket = cfg["sources"]["aws_s3"]["bucket"]
    client = get_s3_client(config_path)

    logger.info(f"Downloading s3://{bucket}/{s3_key}")
    obj = client.get_object(Bucket=bucket, Key=s3_key)
    df = pd.read_csv(io.BytesIO(obj["Body"].read()), dtype=str)
    logger.info(f"Loaded {len(df):,} rows from S3")
    return df


def extract_s3_parquet(s3_key: str, config_path: str = "config/config.yaml") -> pd.DataFrame:
    """Download a Parquet file from S3 and return as DataFrame."""
    with open(config_path) as f:
        cfg = yaml.safe_load(f)

    bucket = cfg["sources"]["aws_s3"]["bucket"]
    client = get_s3_client(config_path)

    logger.info(f"Downloading parquet: s3://{bucket}/{s3_key}")
    obj = client.get_object(Bucket=bucket, Key=s3_key)
    df = pd.read_parquet(io.BytesIO(obj["Body"].read()))
    logger.info(f"Loaded {len(df):,} rows from S3 parquet")
    return df


if __name__ == "__main__":
    files = list_s3_files("claims/raw/2024/")
    if files:
        df = extract_s3_csv(files[0])
        print(df.head())
