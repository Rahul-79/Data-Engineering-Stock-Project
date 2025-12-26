import os
import snowflake.connector
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# --- CONFIGURATION (Redacted for Security) ---
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DB = os.getenv("SNOWFLAKE_DB")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
BUCKET_NAME = os.getenv("AWS_S3_BUCKET_NAME", "my-snowflake-export-bucket")

def export_view_to_s3(view_name, s3_folder_name):
    """
    Unloads refined Gold tables from Snowflake to S3 in Parquet format.
    This enables Databricks to perform advanced analytics on the refined data.
    """
    print(f"--- Starting export for {view_name} to S3 ---")
    
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
        s3_path = f"s3://{BUCKET_NAME}/exports/{s3_folder_name}/"
        
        # Snowflake COPY INTO <location> command
        # Uses Parquet for optimized storage and Snappy compression for fast I/O
        query = f"""
            COPY INTO '{s3_path}'
            FROM {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.{view_name}
            CREDENTIALS=(AWS_KEY_ID='{AWS_ACCESS_KEY}' AWS_SECRET_KEY='{AWS_SECRET_KEY}')
            FILE_FORMAT=(TYPE=PARQUET COMPRESSION=SNAPPY)
            ENCRYPTION=(TYPE='AWS_SSE_S3')
            HEADER=TRUE
            OVERWRITE=TRUE
            SINGLE=FALSE;
        """
        
        cur.execute(query)
        print(f"SUCCESS: Exported {view_name} to {s3_path}")

    except Exception as e:
        print(f"FAILURE: Error exporting {view_name}: {str(e)}")
        raise e
    finally:
        cur.close()
        conn.close()

# --- DAG DEFINITION ---
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2025, 1, 1),
}

with DAG(
    "snowflake_to_s3_export",
    default_args=default_args,
    description="Exports Gold-layer views from Snowflake to AWS S3 for Databricks Analysis",
    schedule_interval="@daily",
    catchup=False
) as dag:

    # Task 1: Candlestick data for technical analysis
    t1 = PythonOperator(
        task_id="export_gold_candlestick",
        python_callable=export_view_to_s3,
        op_kwargs={"view_name": "gold_candlestick", "s3_folder_name": "gold_candlestick"}
    )

    # Task 2: Core KPI metrics
    t2 = PythonOperator(
        task_id="export_gold_kpi",
        python_callable=export_view_to_s3,
        op_kwargs={"view_name": "gold_kpi", "s3_folder_name": "gold_kpi"}
    )

    # Task 3: Treechart data for sector visualization
    t3 = PythonOperator(
        task_id="export_gold_treechart",
        python_callable=export_view_to_s3,
        op_kwargs={"view_name": "gold_treechart", "s3_folder_name": "gold_treechart"}
    )

    # Execution Logic: Parallel export for maximum efficiency
    [t1, t2, t3]
