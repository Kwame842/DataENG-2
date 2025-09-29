import sys
import logging
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


# Import ETL step functions
from extract_metadata import extract_metadata  # Now handles both metadata + streaming batches
from validate import validate_files
from transform_kpis import transform_kpis
from transform_kpis import main
from load_redshift import load_to_redshift

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def on_failure_callback(context):
    """Log task failure details for debugging."""
    task_id = context['task_instance'].task_id
    dag_id = context['dag'].dag_id
    exception = context.get('exception')
    logging.error(f"Task {task_id} in DAG {dag_id} failed: {str(exception)}")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    # 'email': ['your.email@example.com'],  # Enable if SMTP is configured
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': on_failure_callback
}

dag = DAG(
    dag_id='music_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for music streaming service using MWAA',
    schedule_interval='@hourly',
    start_date=days_ago(1),
    catchup=False,
    tags=['etl', 'music', 'mwaa']
)

with dag:
    task_extract_metadata = PythonOperator(
        task_id='extract_metadata',
        python_callable=extract_metadata
    )

    task_validate = PythonOperator(
        task_id='validate_datasets',
        python_callable=validate_files
    )

    task_transform = PythonOperator(
        task_id='transform_kpis',
        python_callable=main,
        dag=dag
    )

    task_load = PythonOperator(
        task_id='load_to_redshift',
        python_callable=load_to_redshift
    )

    # Task dependencies
    task_extract_metadata >> task_validate >> task_transform >> task_load
