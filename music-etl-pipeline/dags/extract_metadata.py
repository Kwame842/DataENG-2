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

def convert_csv_to_parquet_and_upload(source_bucket, source_key, dest_bucket, dest_key, s3_client):
    try:
        logging.info(f"Reading CSV from s3://{source_bucket}/{source_key}")
        response = s3_client.get_object(Bucket=source_bucket, Key=source_key)
        df = pd.read_csv(response['Body'])
        logging.info(f"Successfully read {source_key} into DataFrame")
    except Exception as e:
        logging.error(f"Failed to read {source_key}: {str(e)}")
        raise

    try:
        buffer = BytesIO()
        df.to_parquet(buffer, index=False, engine='pyarrow')
        buffer.seek(0)

        logging.info(f"Uploading Parquet to s3://{dest_bucket}/{dest_key}")
        s3_client.put_object(Bucket=dest_bucket, Key=dest_key, Body=buffer.getvalue())
        logging.info(f"Successfully uploaded {dest_key}")
    except Exception as e:
        logging.error(f"Failed to upload Parquet for {source_key}: {str(e)}")
        raise

def extract_metadata():
    logging.info("Starting metadata extraction for users and songs")

    # Environment variables
    source_bucket = os.getenv('BUCKET_NAME', 'mwaa-bucket01')
    region_name = os.getenv('AWS_REGION', 'eu-north-1')
    dest_bucket = os.getenv('DEST_BUCKET', 'mwaa-bucket02')

    # Keys for source and destination
    files = [
        {
            "source_key": "metadata/users.csv",
            "dest_key": "processed/users.parquet"
        },
        {
            "source_key": "metadata/songs.csv",
            "dest_key": "processed/songs.parquet"
        }
    ]

    # Validate inputs
    if not all([source_bucket, dest_bucket]):
        raise ValueError("Missing required environment variables for S3 buckets")

    # Initialize S3 client
    s3 = boto3.client('s3', region_name=region_name)

    # Loop through files and process
    for file in files:
        convert_csv_to_parquet_and_upload(
            source_bucket=source_bucket,
            source_key=file['source_key'],
            dest_bucket=dest_bucket,
            dest_key=file['dest_key'],
            s3_client=s3
        )

if __name__ == "__main__":
    extract_metadata()
