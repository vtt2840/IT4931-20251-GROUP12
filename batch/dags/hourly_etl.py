from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'group12',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Cron expression "0 * * * *" nghĩa là chạy vào phút thứ 0 của mỗi giờ
with DAG(
    dag_id='1_hourly_cleaning_and_report',
    default_args=default_args,
    description='Clean data, Run Hourly Aggregates and DQ Report',
    schedule_interval='0 * * * *', 
    start_date=days_ago(1),
    catchup=True,
    tags=['hourly', 'spark'],
) as dag:

    # Bước 1: Làm sạch dữ liệu (Chạy mỗi giờ để có dữ liệu sạch mới nhất)
    clean_task = SparkSubmitOperator(
        task_id='clean_data',
        application='/opt/airflow/scripts/cleaner.py',
        conn_id='spark_default',
        conf={"spark.master": "local[*]"}
    )

    # Bước 2: Tổng hợp theo giờ
    hourly_task = SparkSubmitOperator(
        task_id='hourly_aggregates',
        application='/opt/airflow/scripts/batch_hourly_aggregates.py',
        conn_id='spark_default',
        conf={"spark.master": "local[*]"}
    )

    # Bước 3: Kiểm tra chất lượng dữ liệu
    dq_report_task = SparkSubmitOperator(
        task_id='dq_report',
        application='/opt/airflow/scripts/dq_report.py',
        conn_id='spark_default',
        conf={"spark.master": "local[*]"}
    )

    # Luồng chạy: Clean -> Hourly -> DQ Report
    clean_task >> hourly_task >> dq_report_task