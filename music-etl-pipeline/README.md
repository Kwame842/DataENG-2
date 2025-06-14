# 🎵 Music Streaming ETL Pipeline (AWS MWAA)

## 📌 Overview

This project implements an **end-to-end ETL (Extract, Transform, Load) pipeline** for a simulated music streaming service using **Amazon MWAA (Managed Workflows for Apache Airflow)**. The pipeline extracts music metadata and streaming activity data, performs data validation and transformation, and loads the computed KPIs into **Amazon Redshift** for analysis.

## ⚙️ Architecture

```
        +-------------+        +--------------+
        |   RDS CSVs  |        |   S3 Bucket  |
        | (metadata)  |        | (stream logs)|
        +------+------+        +------+-------+
               |                       |
               |                       |
         +-----v-----+          +------v------+
         | Extract   | <------- | Streaming   |
         | Metadata  |          | Batches     |
         +-----+-----+          +------+-------+
               |                       |
               +-----------+-----------+
                           |
                     +-----v-----+
                     | Validate  |
                     +-----+-----+
                           |
                     +-----v-----+
                     | Transform |
                     |   KPIs    |
                     +-----+-----+
                           |
                     +-----v-----+
                     |   Load     |
                     | Redshift   |
                     +-----------+
```

## 🚀 Features

- **Metadata Extraction** from simulated RDS CSVs.
- **Streaming Data Extraction** from hourly batches in S3.
- **File Validation**: Checks for missing or corrupt files.
- **KPI Transformation**: Computes key metrics (e.g., most streamed songs, average session duration).
- **Redshift Loading**: Final KPIs are stored in Amazon Redshift (Presentation Layer).
- Fully orchestrated using **Airflow on MWAA**.

## 📁 Project Structure

```
music-etl-pipeline/
├── dags/
│   ├── music_etl_pipeline.py      # Main Airflow DAG
│   ├── extract_metadata.py        # Extract logic
│   ├── validate.py                # File validation
│   ├── transform_kpis.py          # Transform logic
│   └── load_redshift.py           # Load logic
├── requirements.txt               # Python dependencies
├── diagram/                       # Pipeline diagram
├── screenshots/                   # Screenshots of pipeline execution
└── README.md                      # Project documentation
```

## 🔧 Installation & Setup

### 1. Upload Code to S3

Place all files from `dags/` in your MWAA DAGs S3 folder, e.g.:

```
s3://your-bucket/airflow/dags/
```

### 2. Define Python Dependencies

Create a `requirements.txt`:

```txt
pandas
boto3
psycopg2-binary
sqlalchemy
```

Upload it to S3:

```
s3://your-bucket/airflow/requirements.txt
```

Update MWAA config:

- Open AWS Console > MWAA > Your Environment
- Under _Python requirements file_, specify:

  ```
  s3://your-bucket/airflow/requirements.txt
  ```

Save and wait for MWAA to install dependencies.

### 3. Set Up Connections and Variables

#### Airflow Connections

- `redshift_conn_id`: Redshift connection with login credentials
- (Optional) S3 connection if using non-default IAM roles

#### Airflow Variables

- `s3_bucket_name`: Name of your S3 bucket
- `metadata_prefix`: S3 path to metadata CSVs
- `streaming_prefix`: S3 path to streaming data (e.g. `streaming/`)

## 🧠 DAG Details

| Task ID             | Description                                              |
| ------------------- | -------------------------------------------------------- |
| `extract_metadata`  | Extracts metadata and streaming batches                  |
| `validate_datasets` | Validates the presence and schema of input files         |
| `transform_kpis`    | Transforms the data and computes key performance metrics |
| `load_to_redshift`  | Loads the transformed KPIs to the Redshift warehouse     |

## 📈 KPIs Computed

- Total Streams Per Song
- Unique Listeners Per Song
- Total Listening Time Per User
- Average Session Duration
- Daily/Hourly Active Users

## 🧪 Testing

To test locally:

```bash
pip install -r requirements.txt
airflow dags test music_etl_pipeline 2025-06-12
```

## 🛡️ Monitoring

MWAA provides logging and monitoring through:

- **CloudWatch Logs** (per task)
- **DAG Runs & Task Instances** in Airflow UI

Enable failure notifications via `on_failure_callback` for email or Slack alerting (requires extra configuration).

## 📝 Future Improvements

- Add unit tests using `pytest`
- Automate schema validation using `great_expectations`
- Implement data quality checks post-load
- Enable parallel processing for high-volume batches
- Add CI/CD integration with GitHub Actions or CodePipeline

## 📚 References

- [MWAA Documentation](https://docs.aws.amazon.com/mwaa/latest/userguide/what-is-mwaa.html)
- [Airflow Documentation](https://airflow.apache.org/docs/)
- [Amazon Redshift](https://docs.aws.amazon.com/redshift/)
- [pandas Documentation](https://pandas.pydata.org/)
