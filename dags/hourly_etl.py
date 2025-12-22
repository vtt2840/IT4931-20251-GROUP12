from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import os

default_args = {
    'owner': 'group12',
    'depends_on_past': False, 
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='1_hourly_cleaning_and_report',
    default_args=default_args,
    description='Clean data -> Hourly Aggregates',
    schedule_interval='0 * * * *',  # Chạy mỗi giờ
    start_date=days_ago(1),
    catchup=False, 
    max_active_runs=2, # Tăng lên 2 để chạy nhanh hơn nếu máy khỏe
    tags=['hourly', 'spark'],
) as dag:

    spark_conf_low_resource = {
        "spark.master": "local[1]",          
        "spark.driver.memory": "512m",       
        "spark.executor.memory": "512m",     
        "spark.sql.shuffle.partitions": "10" 
    }

    # TASK 1: CLEANER
    clean_task = SparkSubmitOperator(
        task_id='clean_data',
        application='/opt/airflow/batch/cleaner.py', 
        conn_id='spark_default',
        conf=spark_conf_low_resource,
        verbose=True,
        application_args=[
            "--year", "{{ execution_date.strftime('%Y') }}",
            "--month", "{{ execution_date.strftime('%m') }}",
            "--day", "{{ execution_date.strftime('%d') }}",
            "--hour", "{{ execution_date.strftime('%H') }}"
        ]
    )

    # TASK 2: HOURLY AGGREGATES
    hourly_task = SparkSubmitOperator(
        task_id='hourly_aggregates',
        application='/opt/airflow/batch/batch_hourly_aggregates.py',
        conn_id='spark_default',
        conf=spark_conf_low_resource, 
        verbose=True,
        application_args=[
            "--year", "{{ execution_date.strftime('%Y') }}",
            "--month", "{{ execution_date.strftime('%m') }}",
            "--day", "{{ execution_date.strftime('%d') }}",
            "--hour", "{{ execution_date.strftime('%H') }}"
        ]
    )

    # TASK 3: EXPORT HOURLY (Cần thêm args!)
    export_hourly = SparkSubmitOperator(
        task_id='export_hourly_to_es',
        application='/opt/airflow/visualization/export_hourly.py',
        conn_id='spark_default',
        packages="org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0", 
        conf=spark_conf_low_resource,
        # QUAN TRỌNG: Phải truyền ngày giờ vào đây
        application_args=[
            "--year", "{{ execution_date.strftime('%Y') }}",
            "--month", "{{ execution_date.strftime('%m') }}",
            "--day", "{{ execution_date.strftime('%d') }}",
            "--hour", "{{ execution_date.strftime('%H') }}"
        ]
    )

    # TASK 4: EXPORT CORRELATION (Cần thêm args!)
    export_correlation_hourly = SparkSubmitOperator(
        task_id='export_hourly_correlation_to_es',
        application='/opt/airflow/visualization/export_weather_aqi_relation.py',
        packages="org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0", 
        conn_id='spark_default',
        conf=spark_conf_low_resource,
        # QUAN TRỌNG: Phải truyền ngày giờ vào đây
        application_args=[
            "--year", "{{ execution_date.strftime('%Y') }}",
            "--month", "{{ execution_date.strftime('%m') }}",
            "--day", "{{ execution_date.strftime('%d') }}",
            "--hour", "{{ execution_date.strftime('%H') }}"
        ]
    )

    clean_task >> hourly_task >> [export_hourly, export_correlation_hourly]