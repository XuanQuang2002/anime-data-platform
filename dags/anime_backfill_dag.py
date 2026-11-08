from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
import sys

# Add src to path so airflow can import it
sys.path.append('/opt/airflow/')

from src.extract import extract_to_bronze
from src.transform import clean_to_silver, check_quality
from src.load import load_to_gold

default_args = {
    'owner': 'admin',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['data_team@example.com'],
}

def print_execution_report(**kwargs):
    """
    Generate a summary report of the pipeline execution (Monitoring step).
    """
    print("="*50)
    print(" 🚀 ANIME DATA PIPELINE EXECUTION REPORT 🚀")
    print("="*50)
    print(f"DAG Run ID: {kwargs['dag_run'].run_id}")
    print(f"Logical Date: {kwargs['logical_date']}")
    print("Status: SUCCESS ✅")
    print("Pipeline has successfully extracted Data Lake files,")
    print("passed Data Contracts (Great Expectations),")
    print("and loaded into PostgreSQL Star Schema Data Mart.")
    print("="*50)

with DAG(
    'anime_backfill_dag',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    description='End-to-End ETL for Anime Data using Medallion Architecture'
) as dag:
    
    # TODO: Khai báo EmptyOperator cho task_id='start'
    
    # TODO: Khai báo PythonOperator cho task_id='extract_to_bronze'
    
    # TODO: Khai báo PythonOperator cho task_id='transform_to_silver'
    
    # TODO: Khai báo BranchPythonOperator cho task_id='check_quality'
    
    # TODO: Khai báo PythonOperator cho task_id='load_to_gold'
    
    # TODO: Khai báo PythonOperator cho task_id='execution_report' (sử dụng print_execution_report)
    
    # TODO: Khai báo EmptyOperator cho task_id='alert_quality'
    
    # TODO: Khai báo EmptyOperator cho task_id='end', lưu ý trigger_rule='none_failed'
    
    
    # YOUR CODE HERE
    # start = ...
    # t_extract = ...
    # t_transform = ...
    # t_branch = ...
    # t_load = ...
    # t_report = ...
    # t_alert = ...
    # end = ...
    
    # TODO: Khai báo sự phụ thuộc (Dataflow) giữa các task
    # Flow 1: start -> extract -> transform -> branch
    # Flow 2 (branch success): branch -> load -> report -> end
    # Flow 3 (branch failed): branch -> alert -> end
    
    # YOUR CODE HERE
    pass
