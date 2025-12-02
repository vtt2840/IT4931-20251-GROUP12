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
    dag_id='3_dq_report_job',                   
    default_args=default_args,
    description='Run Data Quality Report daily',
    schedule_interval='30 0 * * *',    
    start_date=days_ago(1),  
    catchup=False,                     
    max_active_runs=1,                 
    tags=['daily', 'spark'],
) as dag:
    dq_report_task = SparkSubmitOperator(
        task_id='dq_report',
        application='/opt/airflow/scripts/dq_report.py',
        conn_id='spark_default',
        conf={
            "spark.master": "local[1]",         
            "spark.driver.memory": "512m",       
            "spark.executor.memory": "512m",     
            "spark.sql.shuffle.partitions": "10" 
        },
        verbose=True
    )

    dq_report_task

   