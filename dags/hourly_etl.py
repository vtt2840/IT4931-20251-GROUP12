from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'group12',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='1_hourly_cleaning_and_report',
    default_args=default_args,
    description='Clean data -> Hourly Aggregates',
    schedule_interval='0 * * * *', 
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=['hourly', 'spark'],
) as dag:

    spark_conf_low_resource = {
        "spark.master": "local[1]",          # QUAN TRỌNG: Chỉ dùng 1 Core (thay vì *)
        "spark.driver.memory": "512m",       # Tăng lên 512m (300m là không đủ)
        "spark.executor.memory": "512m",     
        "spark.sql.shuffle.partitions": "10" # Giảm partition
    }

    # Bước 1: Làm sạch dữ liệu
    clean_task = SparkSubmitOperator(
        task_id='clean_data',
        application='/opt/airflow/scripts/cleaner.py',
        conn_id='spark_default',
        conf=spark_conf_low_resource,
        verbose=True
    )

    # Bước 2: Tổng hợp theo giờ
    hourly_task = SparkSubmitOperator(
        task_id='hourly_aggregates',
        application='/opt/airflow/scripts/batch_hourly_aggregates.py',
        conn_id='spark_default',
        conf=spark_conf_low_resource, 
        verbose=True
    )

    # Luồng chạy
    clean_task >> hourly_task