from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

def test_bigquery_connection():
    """Test BigQuery connection and run simple query"""
    hook = BigQueryHook(gcp_conn_id='bigquery_default')
    client = hook.get_client()
    
    # Simple query to test connection
    query = "SELECT 1 as test_value, 'Hello BigQuery' as message"
    results = client.query(query)
    
    for row in results:
        print(f"Test query result: {row.test_value} - {row.message}")
    
    return "BigQuery connection test successful!"

default_args = {
    'owner': 'data-team',
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

dag = DAG(
    'test_bigquery_connection',
    default_args=default_args,
    description='Test BigQuery connection through Airflow',
    schedule_interval=None,
    catchup=False,
    tags=['bigquery', 'connection-test']
)

test_task = PythonOperator(
    task_id='test_bigquery_connection',
    python_callable=test_bigquery_connection,
    dag=dag
)