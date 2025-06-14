import pandas as pd
import psycopg2
import os
import logging
import s3fs
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_to_redshift():
    logger.info("Loading KPIs and Metadata to Redshift")

    # Redshift connection details from .env
    db_params = {
        'dbname': os.getenv('REDSHIFT_DB'),
        'user': os.getenv('REDSHIFT_USER'),
        'password': os.getenv('REDSHIFT_PASSWORD'),
        'host': os.getenv('REDSHIFT_HOST'),
        'port': int(os.getenv('REDSHIFT_PORT', 5439))
    }

    # S3 paths
    s3_processed = 'mwaa-bucket02/processed'
    s3_output = 'mwaa-bucket02/output'
    s3_metadata = 'mwaa-bucket02/metadata'  # Folder for extracted metadata
    s3 = s3fs.S3FileSystem()

    def map_dtype(dtype):
        if pd.api.types.is_integer_dtype(dtype):
            return 'BIGINT'
        elif pd.api.types.is_float_dtype(dtype):
            return 'DOUBLE PRECISION'
        elif pd.api.types.is_bool_dtype(dtype):
            return 'BOOLEAN'
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            return 'TIMESTAMP'
        else:
            return 'VARCHAR(256)'

    def load_parquet_to_redshift(df, table_name, cur):
        logger.info(f"Loading {table_name} into Redshift...")
        temp_table = f"stg_{table_name}"
        join_col = df.columns[0]

        cur.execute(f"DROP TABLE IF EXISTS {temp_table};")
        cur.execute(f"CREATE TEMP TABLE {temp_table} (LIKE {table_name});")

        for _, row in df.iterrows():
            values = tuple(row)
            placeholders = ','.join(['%s'] * len(values))
            cur.execute(f"INSERT INTO {temp_table} VALUES ({placeholders});", values)

        cur.execute(f"""
            DELETE FROM {table_name}
            USING {temp_table}
            WHERE {table_name}.{join_col} = {temp_table}.{join_col};
        """)
        cur.execute(f"INSERT INTO {table_name} SELECT * FROM {temp_table};")
        logger.info(f"{table_name} updated successfully.")

    try:
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()

        # Load KPI files
        for kpi_file, table_name in [
            ('genre_kpis.parquet', 'genre_kpis'),
            ('hourly_kpis.parquet', 'hourly_kpis')
        ]:
            kpi_path = f"{s3_processed}/{kpi_file}"
            if s3.exists(kpi_path):
                with s3.open(kpi_path, 'rb') as f:
                    df = pd.read_parquet(f, engine='pyarrow')
                load_parquet_to_redshift(df, table_name, cur)
            else:
                logger.warning(f"{kpi_path} does not exist.")

        # Load dynamic output files
        output_files = s3.ls(s3_output)
        for file in output_files:
            if file.endswith('.parquet'):
                table_name = os.path.basename(file).replace('.parquet', '')
                try:
                    with s3.open(file, 'rb') as f:
                        df = pd.read_parquet(f, engine='pyarrow')

                    create_cols = ", ".join([
                        f"{col} {map_dtype(dtype)}"
                        for col, dtype in zip(df.columns, df.dtypes)
                    ])
                    cur.execute(f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            {create_cols}
                        );
                    """)
                    load_parquet_to_redshift(df, table_name, cur)

                except Exception as e:
                    logger.error(f"Failed to load output file {file}: {e}")
                    continue

        # Load metadata files (NEW)
        metadata_files = s3.ls(s3_metadata)
        for file in metadata_files:
            if file.endswith('.parquet'):
                table_name = os.path.basename(file).replace('.parquet', '')
                try:
                    with s3.open(file, 'rb') as f:
                        df = pd.read_parquet(f, engine='pyarrow')

                    create_cols = ", ".join([
                        f"{col} {map_dtype(dtype)}"
                        for col, dtype in zip(df.columns, df.dtypes)
                    ])
                    cur.execute(f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            {create_cols}
                        );
                    """)
                    load_parquet_to_redshift(df, table_name, cur)

                except Exception as e:
                    logger.error(f"Failed to load metadata file {file}: {e}")
                    continue

        conn.commit()
        logger.info("All data committed to Redshift.")

    except Exception as e:
        logger.error(f"Error: {e}")
        raise
    finally:
        if 'cur' in locals(): cur.close()
        if 'conn' in locals(): conn.close()
        logger.info("Redshift connection closed.")
