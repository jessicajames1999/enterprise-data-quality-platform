from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator

# Simple test DAG - just trigger dbt job
dag = DAG(
    'test_dbt_job_trigger',
    default_args={
        'owner': 'data-team',
        'start_date': datetime(2024, 1, 1),
        'retries': 1
    },
    description='Simple test to trigger dbt Cloud job',
    schedule_interval=None,
    catchup=False,
    tags=['dbt', 'test']
)

# Single task to test dbt job trigger
test_dbt_trigger = SimpleHttpOperator(
    task_id='test_trigger_dbt',
    http_conn_id='dbt_cloud_default',
    endpoint='/api/v2/accounts/262085/jobs/921026/run/',
    method='POST',
    headers={
        'Authorization': 'Token {{ conn.dbt_cloud_default.password }}',
        'Content-Type': 'application/json'
    },
    data='{"cause": "Test trigger from Airflow"}',
    dag=dag
)