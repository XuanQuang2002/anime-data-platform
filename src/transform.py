import structlog
import polars as pl
import os
from pathlib import Path

logger = structlog.get_logger()

BRONZE_DIR = "/opt/airflow/data/bronze"
SILVER_DIR = "/opt/airflow/data/silver"
DLQ_DIR = "/opt/airflow/data/dlq"

def clean_to_silver(**kwargs):
    """
    Clean data from Bronze layer (Parquet) and load into Silver layer (Parquet).
    Implement Data Contracts and DLQ for invalid data.
    """
    run_id = kwargs.get('dag_run', None)
    log = logger.bind(run_id=run_id.run_id if run_id else "manual")
    
    os.makedirs(SILVER_DIR, exist_ok=True)
    os.makedirs(DLQ_DIR, exist_ok=True)
    
    try:
        # TODO: Đọc các file parquet từ BRONZE_DIR vào các DataFrame của polars
        # Gợi ý: pl.read_parquet(f"{BRONZE_DIR}/<table_name>.parquet")
        
        # YOUR CODE HERE
        # customers_df = ...
        # orders_df = ...
        # order_items_df = ...
        # products_df = ...
        # sellers_df = ...
        # geolocation_df = ...
        # payments_df = ...
        # reviews_df = ...
        # translation_df = ...

        
        # 1. Clean Customers
        log.info("Cleaning Customers...")
        # TODO: Loại bỏ các dòng trùng lặp theo customer_id
        # TODO: Chuẩn hóa cột customer_city thành chữ thường (lowercase)
        # TODO: Chuẩn hóa cột customer_state thành chữ HOA (uppercase)
        # TODO: Join với bảng geolocation_df (đã được aggregate theo zip_code_prefix) để lấy lat/lng
        # TODO: Lưu kết quả thành dim_customers.parquet vào SILVER_DIR
        
        # YOUR CODE HERE
        
        
        # 2. Clean Sellers
        log.info("Cleaning Sellers...")
        # TODO: Loại bỏ các dòng trùng lặp và null ở seller_id
        # TODO: Join với geolocation_df để lấy lat/lng
        # TODO: Lưu kết quả thành dim_sellers.parquet vào SILVER_DIR
        
        # YOUR CODE HERE
        
        
        # 3. Clean Products
        log.info("Cleaning Products...")
        # TODO: Loại bỏ các dòng trùng lặp ở product_id và fill null bằng strategy forward
        # TODO: Đổi tên các cột bị sai chính tả (vd: product_name_lenght -> product_name_length)
        # TODO: Join với translation_df để lấy tên danh mục tiếng anh
        # TODO: Lưu kết quả thành dim_products.parquet vào SILVER_DIR
        
        # YOUR CODE HERE
        
        
        # 4. Transform Fact Order Items
        log.info("Transforming Fact Order Items...")
        # TODO: Lọc các đơn hàng hợp lệ (vd: nếu delivered thì phải có đủ các trường ngày tháng)
        # TODO: Join order_items_df với các đơn hàng hợp lệ (orders_df) để tạo fct_items_df
        # TODO: Kiểm tra Data Contract (price > 0 và freight_value >= 0)
        # TODO: Lưu các record HỢP LỆ vào fct_order_items.parquet trong SILVER_DIR
        # TODO: Đưa các record KHÔNG HỢP LỆ vào thư mục DLQ (Dead Letter Queue)
        
        # YOUR CODE HERE
        
        
        # 5. Transform Fact Order Payments
        log.info("Transforming Fact Order Payments...")
        # TODO: Join payments_df với orders_df (lấy order_id, customer_id)
        # TODO: Loại bỏ trùng lặp
        # TODO: Lưu fct_order_payments.parquet
        
        # YOUR CODE HERE
        
        
        # 6. Transform Fact Order Reviews
        log.info("Transforming Fact Order Reviews...")
        # TODO: Join reviews_df với orders_df
        # TODO: Loại bỏ trùng lặp
        # TODO: Lưu fct_order_reviews.parquet
        
        # YOUR CODE HERE


        # Lưu thông tin chất lượng dữ liệu để task check_quality kiểm tra
        # TODO: Cập nhật biến pass_quality. Ví dụ: pass_quality = True nếu số record bị lỗi < 10%
        pass_quality = True 
        kwargs['ti'].xcom_push(key="pass_quality", value=pass_quality)
        
        log.info("transform_silver_success")
                 
    except Exception as e:
        log.exception("transform_silver_failed")
        raise e

def check_quality(**kwargs):
    """
    Perform Data Quality checks on Silver data (inspired by Great Expectations).
    """
    passed = kwargs['ti'].xcom_pull(task_ids="transform_silver", key="pass_quality")
    log = logger.bind(task="check_quality")
    
    try:
        # TODO: Thực hiện Data Quality check dựa vào thư viện polars
        # 1. Đọc file dim_customers.parquet
        # 2. Kiểm tra số lượng dòng (phải lớn hơn 0)
        # 3. Kiểm tra xem có customer_id nào bị null không
        # 4. Kiểm tra tính duy nhất của customer_id (số lượng unique bằng tổng số record)
        # Nếu bất kỳ check nào fail, gán passed = False
        
        # YOUR CODE HERE
        
        # TODO: Kiểm tra với bảng fct_order_items.parquet
        # Kiểm tra order_status chỉ thuộc tập giá trị hợp lệ 
        # (vd: ["delivered", "shipped", "invoiced", "processing", "approved", "canceled", "unavailable", "created"])
        
        # YOUR CODE HERE

        if passed:
            log.info("All Data Quality checks passed successfully!")
            return "load_to_gold"
        else:
            log.warning("Data Quality checks failed. Routing to alert.")
            return "alert_quality"
            
    except Exception as e:
        log.exception(f"Data Quality check encountered an error: {e}")
        return "alert_quality"

if __name__ == "__main__":
    # For local testing
    clean_to_silver(ti=type('obj', (object,), {'xcom_push': lambda **kw: None})())
