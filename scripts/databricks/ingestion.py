import urllib.parse
import os

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
BUCKET_NAME = os.getenv("AWS_S3_BUCKET_NAME")

encoded_secret_key = urllib.parse.quote(AWS_SECRET_KEY, safe="")

spark.sql("CREATE SCHEMA IF NOT EXISTS stocks.stock_market_db")

key = f"s3a://{AWS_ACCESS_KEY}:{encoded_secret_key}@{BUCKET_NAME}"

def sync_s3_to_table(folder_name, table_name):
    s3_path = f"{key}/exports/{folder_name}/"
    df = spark.read.parquet(s3_path)
    df.write.mode("overwrite").saveAsTable(f"stocks.stock_market_db.{table_name}")

sync_s3_to_table("gold_candlestick", "gold_candlestick")
sync_s3_to_table("gold_kpi", "gold_kpi")
sync_s3_to_table("gold_treechart", "gold_treechart")
