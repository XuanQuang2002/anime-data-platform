import structlog
import polars as pl
import os
from pathlib import Path

logger = structlog.get_logger()

RAW_DIR = "/opt/airflow/data/raw"
BRONZE_DIR = "/opt/airflow/data/bronze"

def extract_to_bronze(**kwargs):
    """
    Extract data from raw CSV files and load into Bronze layer as Parquet files.
    """
    run_id = kwargs.get('dag_run', None)
    log = logger.bind(run_id=run_id.run_id if run_id else "manual")
    
    os.makedirs(BRONZE_DIR, exist_ok=True)
    
    # List of tables to extract
    tables = [
        "customers_dataset",
        "orders_dataset",
        "order_items_dataset",
        "products_dataset",
        "sellers_dataset",
        "geolocation_dataset",
        "order_payments_dataset",
        "order_reviews_dataset",
        "product_category_name_translation"
    ]
    
    try:
        extracted_tables = []
        for table in tables:
            csv_path = f"{RAW_DIR}/{table}.csv"
            parquet_path = f"{BRONZE_DIR}/{table}.parquet"
            
            if not os.path.exists(csv_path):
                log.warning(f"Raw file not found: {csv_path}")
                continue
                
            log.info(f"Extracting {table} to Bronze layer...")
            
            # TODO: Sinh viên thêm code sử dụng thư viện polars (pl) để đọc file CSV từ csv_path 
            # (Gợi ý: xử lý lỗi khi đọc bằng tham số ignore_errors=True)
            # Sau đó ghi DataFrame thành file Parquet vào parquet_path.
            
            # YOUR CODE HERE
            # df = ...
            # df.write_parquet(...)
            
            extracted_tables.append(table)
            log.info(f"Successfully extracted {table}")
            
        kwargs['ti'].xcom_push(key="extracted_tables", value=extracted_tables)
        log.info("extract_bronze_success", tables=extracted_tables)
    except Exception as e:
        log.exception("extract_bronze_failed")
        raise e

if __name__ == "__main__":
    # For local testing
    extract_to_bronze(ti=type('obj', (object,), {'xcom_push': lambda **kw: None})())
