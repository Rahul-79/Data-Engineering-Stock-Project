import os
import boto3
import snowflake.connector
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# CONFIGURATION: Fetching from environment variables (redacted for Git)
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password123")
BUCKET = "bronze-transactions"
LOCAL_DIR = "/tmp/minio_downloads"

SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DB = os.getenv("SNOWFLAKE_DB")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

def download_from_minio():
    """Downloads new JSON files from MinIO Landing Zone to local Airflow worker storage."""
    os.makedirs(LOCAL_DIR, exist_ok=True)
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )
    
    # List objects in the bronze bucket
    objects = s3.list_objects_v2(Bucket=BUCKET).get("Contents", [])
    local_files = []
    
    for obj in objects:
        key = obj["Key"]
        # Filter for JSON files specifically
        if key.endswith('.json'):
            local_file = os.path.join(LOCAL_DIR, os.path.basename(key))
            s3.download_file(BUCKET, key, local_file)
            print(f"Downloaded {key} -> {local_file}")
            local_files.append(local_file)
            
    return local_files

def load_to_snowflake(**kwargs):
    """Uploads downloaded files to Snowflake Internal Stage and executes COPY INTO."""
    local_files = kwargs['ti'].xcom_pull(task_ids='download_minio')
    
    if not local_files:
        print("No files found in MinIO to load.")
        return

    # Database Connection
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DB,
        schema=SNOWFLAKE_SCHEMA
    )
    cur = conn.cursor()

    try:
        for f in local_files:
            # PUT command moves file to Snowflake Stage
            cur.execute(f"PUT file://{f} @%bronze_stock_quotes_raw AUTO_COMPRESS=TRUE")
            print(f"Uploaded {f} to Snowflake stage")

        # COPY INTO moves data from Stage to the Table
        cur.execute("""
            COPY INTO bronze_stock_quotes_raw
            FROM @%bronze_stock_quotes_raw
            FILE_FORMAT = (TYPE=JSON)
            ON_ERROR = 'CONTINUE';
        """)
        print("Snowflake COPY INTO command completed successfully.")
        
        # Cleanup local files after successful upload
        for f in local_files:
            os.remove(f)
            
    except Exception as e:
        print(f"Snowflake Loading Error: {e}")
        raise e
    finally:
        cur.close()
        conn.close()

# DAG Definition
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1), # Updated for 2025
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "minio_to_snowflake_pipeline",
    default_args=default_args,
    description="Orchestrates data movement from MinIO Landing to Snowflake Bronze",
    schedule_interval="*/10 * * * *", 
    catchup=False,
    max_active_runs=1
) as dag:

    task_extract = PythonOperator(
        task_id="download_minio",
        python_callable=download_from_minio,
    )

    task_load = PythonOperator(
        task_id="load_snowflake",
        python_callable=load_to_snowflake,
    )

    task_extract >> task_load
