import pandas as pd
import os
import logging
import boto3
from botocore.exceptions import ClientError
from io import BytesIO

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def read_csv_from_s3(bucket, key, region='eu-north-1'):
    s3 = boto3.client('s3', region_name=region)
    try:
        logger.info(f"Reading CSV from s3://{bucket}/{key}")
        obj = s3.get_object(Bucket=bucket, Key=key)
        return pd.read_csv(BytesIO(obj['Body'].read()))
    except ClientError as e:
        logger.error(f"Failed to read {key} from {bucket}: {e}")
        raise

def list_streaming_batches(bucket, prefix='streaming/', region='eu-north-1'):
    s3 = boto3.client('s3', region_name=region)
    keys = []
    try:
        paginator = s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get('Contents', []):
                key = obj['Key']
                if key.endswith('.csv'):
                    keys.append(key)
    except ClientError as e:
        logger.error(f"Failed to list streaming files in {bucket}/{prefix}: {e}")
        raise
    logger.info(f"Found {len(keys)} batch files in {bucket}/{prefix}")
    return keys

def extract_metadata():
    logger.info("Extracting metadata and batch stream files")

    bucket = os.getenv('METADATA_BUCKET', 'mwaa-bucket01')
    songs_key = os.getenv('SONGS_KEY', 'metadata/songs.csv')
    streaming_prefix = os.getenv('STREAMING_PREFIX', 'streaming/')
    region = os.getenv('AWS_REGION', 'eu-north-1')

    # Load songs.csv
    songs_df = read_csv_from_s3(bucket, songs_key, region)

    # Load all streaming batch files and concatenate
    batch_keys = list_streaming_batches(bucket, streaming_prefix, region)
    stream_dfs = [read_csv_from_s3(bucket, key, region) for key in batch_keys]
    streams_df = pd.concat(stream_dfs, ignore_index=True)

    logger.info(f"Songs columns: {songs_df.columns.tolist()}")
    logger.info(f"Streams columns: {streams_df.columns.tolist()}")

    for col, df_name in [('track_id', songs_df), ('track_id', streams_df),
                         ('listen_time', streams_df), ('track_genre', songs_df)]:
        if col not in df_name.columns:
            raise ValueError(f"Missing required column: '{col}'")

    return streams_df, songs_df

def transform_kpis(streams_df, songs_df):
    logger.info("Transforming KPIs")

    merged_df = streams_df.merge(songs_df, on='track_id', how='left')
    merged_df['listen_time'] = pd.to_datetime(merged_df['listen_time'], errors='coerce')
    merged_df = merged_df.dropna(subset=['listen_time'])

    merged_df['hour'] = merged_df['listen_time'].dt.hour

    genre_kpis = merged_df.groupby('track_genre').agg(
        total_listens=('track_id', 'count')
    ).reset_index()

    hourly_kpis = merged_df.groupby('hour').agg(
        total_listens=('track_id', 'count')
    ).reset_index()

    return genre_kpis, hourly_kpis

def save_kpis(genre_kpis, hourly_kpis):
    logger.info("Saving KPIs as Parquet files")

    output_bucket = os.getenv('OUTPUT_BUCKET', 'mwaa-bucket02')
    genre_key = os.getenv('GENRE_KPI_KEY', 'output/genre_kpis.parquet')
    hourly_key = os.getenv('HOURLY_KPI_KEY', 'output/hourly_kpis.parquet')
    region = os.getenv('AWS_REGION', 'eu-north-1')

    s3 = boto3.client('s3', region_name=region)

    try:
        for df, key in [(genre_kpis, genre_key), (hourly_kpis, hourly_key)]:
            buffer = BytesIO()
            df.to_parquet(buffer, index=False)
            buffer.seek(0)
            s3.upload_fileobj(buffer, output_bucket, key)
            logger.info(f"Uploaded KPIs to s3://{output_bucket}/{key}")
    except Exception as e:
        logger.error(f"Error saving KPIs to S3: {e}")
        raise

# Main function to be used by Airflow PythonOperator
def main():
    try:
        streams_df, songs_df = extract_metadata()
        genre_kpis, hourly_kpis = transform_kpis(streams_df, songs_df)
        save_kpis(genre_kpis, hourly_kpis)
    except Exception as e:
        logger.error(f"Error in main: {str(e)}")
        raise
