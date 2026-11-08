import structlog
import polars as pl
from sqlalchemy import create_engine, text
import os

logger = structlog.get_logger()

SILVER_DIR = "/opt/airflow/data/silver"

def upsert_table(engine, df, table_name, schema, pk_columns):
    """Helper function to perform UPSERT into PostgreSQL."""
    # TODO: Khởi tạo bảng tạm (temp_table) trong PostgreSQL
    # Gợi ý: Dùng pandas.DataFrame.to_sql(..., if_exists="replace")
    
    # TODO: Tạo câu SQL INSERT ... ON CONFLICT (...) DO UPDATE SET ...
    # để upsert dữ liệu từ bảng tạm vào bảng chính, sau đó DROP bảng tạm.
    
    # YOUR CODE HERE
    pass

def load_to_gold(**kwargs):
    """
    Load data from Silver layer to Gold layer (PostgreSQL Data Mart).
    Uses SQLAlchemy for UPSERT insertion.
    """
    run_id = kwargs.get('dag_run', None)
    log = logger.bind(run_id=run_id.run_id if run_id else "manual")
    
    db_url = "postgresql+psycopg2://admin:password@datamart:5432/ecommerce"
    engine = create_engine(db_url)
    
    try:
        log.info("Loading Dimension: dim_customers...")
        # TODO: Đọc dim_customers.parquet và gọi hàm upsert_table để đưa vào bảng "dim_customers" trong PostgreSQL
        
        # YOUR CODE HERE
        
        log.info("Loading Dimension: dim_products...")
        # TODO: Đọc dim_products.parquet và gọi hàm upsert_table
        
        # YOUR CODE HERE
        
        log.info("Loading Dimension: dim_sellers...")
        # TODO: Đọc dim_sellers.parquet và gọi hàm upsert_table
        
        # YOUR CODE HERE
        
        log.info("Loading Fact: fct_order_items...")
        # TODO: Đọc fct_order_items.parquet
        # Lưu ý chuyển đổi các cột Timestamp về định dạng phù hợp (vd datetime64[ns]) trước khi đẩy lên DB
        # Gọi hàm upsert_table
        
        # YOUR CODE HERE

        log.info("Loading Fact: fct_order_payments...")
        # TODO: Load fct_order_payments
        
        # YOUR CODE HERE

        log.info("Loading Fact: fct_order_reviews...")
        # TODO: Load fct_order_reviews (nhớ xử lý timestamp cho các cột review)
        
        # YOUR CODE HERE
        
        log.info("load_to_gold_success")
                 
    except Exception as e:
        log.exception("load_to_gold_failed")
        raise e

if __name__ == "__main__":
    load_to_gold()
