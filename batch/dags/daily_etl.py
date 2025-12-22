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
    dag_id='2_daily_aggregates_job',
    default_args=default_args,
    description='Run Daily Aggregates only',
    schedule_interval='0 0 * * *',  
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1, 
    tags=['daily', 'spark'],
) as dag:

    # TASK 1: DAILY AGGREGATES
    daily_task = SparkSubmitOperator(
        task_id='daily_aggregates',
        application='/opt/airflow/batch/batch_daily_aggregates.py',
        conn_id='spark_default',
        conf={
            "spark.master": "local[1]",
            "spark.driver.memory": "512m",
             "spark.executor.memory": "512m",
        },  
        application_args=[
            "--year", "{{ execution_date.strftime('%Y') }}",
            "--month", "{{ execution_date.strftime('%m') }}",
            "--day", "{{ execution_date.strftime('%d') }}"
        ]
    )

    # TASK 2: EXPORT DAILY (Cần thêm args!)
    export_daily = SparkSubmitOperator(
        task_id='export_daily_to_es',
        application='/opt/airflow/visualization/export_daily.py',
        conn_id='spark_default',
        packages="org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0",
        conf={
            "spark.master": "local[1]",
            "spark.driver.memory": "512m",
             "spark.executor.memory": "512m"
        },
        # QUAN TRỌNG: Phải truyền ngày tháng
        application_args=[
            "--year", "{{ execution_date.strftime('%Y') }}",
            "--month", "{{ execution_date.strftime('%m') }}",
            "--day", "{{ execution_date.strftime('%d') }}"
        ]
    )

    daily_task >> export_daily
