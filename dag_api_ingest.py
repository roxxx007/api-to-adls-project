from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

notebook_task = {
    'notebook_path': '/Users/your.name@databricks.com/api_to_adls_notebook',
}

with DAG('api_to_adls_pipeline',
         default_args=default_args,
         schedule_interval='@daily',
         start_date=datetime(2024, 1, 1),
         catchup=False) as dag:

    run_ingestion = DatabricksSubmitRunOperator(
        task_id='run_api_ingestion',
        databricks_conn_id='databricks_default',
        existing_cluster_id='YOUR_CLUSTER_ID',
        notebook_task=notebook_task
    )

    run_ingestion
