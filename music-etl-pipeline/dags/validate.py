import boto3
import pandas as pd
import os
import logging
from io import BytesIO
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def validate_files():
    logging.info("Starting cloud-only validation of metadata and streaming batch CSV files from S3")

    # Environment variables
    bucket = os.getenv('METADATA_BUCKET', 'mwaa-bucket01')
    users_key = os.getenv('USERS_KEY', 'metadata/users.csv')
    songs_key = os.getenv('SONGS_KEY', 'metadata/songs.csv')
    stream_prefix = os.getenv('STREAMS_PREFIX', 'streaming/')
    region = os.getenv('AWS_REGION', 'eu-north-1')

    # Required columns
    metadata_columns = {
        users_key: ['user_id'],
        songs_key: ['track_id'],
    }
    streaming_required_columns = ['user_id', 'track_id']

    if not all([bucket, users_key, songs_key, stream_prefix]):
        raise ValueError("Missing required environment variables.")

    # Init S3 client
    s3 = boto3.client('s3', region_name=region)

    # Helper: Load CSV from S3
    def read_csv_from_s3(bucket, key):
        try:
            obj = s3.get_object(Bucket=bucket, Key=key)
            return pd.read_csv(BytesIO(obj['Body'].read()))
        except Exception as e:
            raise RuntimeError(f"Failed to read {key}: {str(e)}")

    # Validate metadata files
    for key, required_cols in metadata_columns.items():
        logging.info(f"Validating metadata file: {key}")
        df = read_csv_from_s3(bucket, key)
        _validate_df(df, required_cols, key)

    # Validate streaming batches
    logging.info(f"Listing streaming batch files under: {stream_prefix}")
    response = s3.list_objects_v2(Bucket=bucket, Prefix=stream_prefix)

    if 'Contents' not in response:
        logging.warning("No streaming batch files found.")
        return

    for obj in response['Contents']:
        key = obj['Key']
        if not key.endswith('.csv'):
            continue

        logging.info(f"Validating streaming batch: {key}")
        df = read_csv_from_s3(bucket, key)
        _validate_df(df, streaming_required_columns, key)

    logging.info("All metadata and streaming files validated successfully.")

# Helper: Validate DataFrame for required columns and missing values
def _validate_df(df, required_columns, source):
    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        raise ValueError(f"{source} is missing columns: {missing_cols}")

    for col in required_columns:
        if df[col].isna().any():
            raise ValueError(f"{source} has missing values in column: {col}")

    logging.info(f"{source} passed validation. Rows: {len(df)}, Columns: {df.columns.tolist()}")

if __name__ == "__main__":
    validate_files()
