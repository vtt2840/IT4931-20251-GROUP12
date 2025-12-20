from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

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
    schedule_interval='0 * * * *', 
    start_date=days_ago(1),
    catchup=False, 
    max_active_runs=1,
    tags=['hourly', 'spark'],
) as dag:

    spark_conf_low_resource = {
        "spark.master": "local[1]",          
        "spark.driver.memory": "512m",       
        "spark.executor.memory": "512m",     
        "spark.sql.shuffle.partitions": "10" 
    }

    # TASK 1: LÀM SẠCH DỮ LIỆU 
    clean_task = SparkSubmitOperator(
        task_id='clean_data',
        application='/opt/airflow/scripts/cleaner.py', 
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

    # TASK 2: TỔNG HỢP THEO GIỜ
    hourly_task = SparkSubmitOperator(
        task_id='hourly_aggregates',
        application='/opt/airflow/scripts/batch_hourly_aggregates.py',
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

    #Export PM2.5 theo giờ
    export_hourly = SparkSubmitOperator(
        task_id='export_hourly_to_es',
        application='/opt/airflow/scripts/export_hourly.py',
        conn_id='spark_default',
        conf=spark_conf_low_resource
    )

    # Export relationship PM2.5 – weather theo giờ
    export_correlation_hourly = SparkSubmitOperator(
        task_id='export_hourly_correlation_to_es',
        application='/opt/airflow/scripts/export_correlation.py',
        conn_id='spark_default',
        conf=spark_conf_low_resource
    )

    # Luồng chạy
    clean_task >> hourly_task >> export_hourly >> export_correlation_hourly