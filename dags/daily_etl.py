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

    daily_task = SparkSubmitOperator(
        task_id='daily_aggregates',
        application='/opt/airflow/scripts/batch_daily_aggregates.py',
        conn_id='spark_default',
        
        conf={
            "spark.master": "local[2]",
            "spark.driver.memory": "1g"
        },  

        application_args=[
            "--year", "{{ execution_date.strftime('%Y') }}"
        ]
    )

    daily_task