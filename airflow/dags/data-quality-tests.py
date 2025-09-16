from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

def test_great_expectations_basic():
    """Basic test to verify GE is working"""
    import great_expectations as gx
    import os
    
    # Change to the GE project directory
    os.chdir('/opt/airflow/great_expectations')
    
    # Get GE context
    context = gx.get_context()
    print(f"Great Expectations version: {gx.__version__}")
    print(f"GE context loaded from: {context.root_directory}")
    
    # List available datasources (should be empty for now)
    datasources = list(context.list_datasources())
    print(f"Available datasources: {datasources}")
    
    return "Great Expectations basic test completed successfully!"

def test_bigquery_connection():
    """Test that BigQuery provider is available"""
    try:
        from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
        print("BigQuery provider imported successfully")
        return "BigQuery provider test passed!"
    except Exception as e:
        print(f"BigQuery provider test failed: {e}")
        raise

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'data_quality_foundation_test',
    default_args=default_args,
    description='Test Great Expectations and BigQuery foundation setup',
    schedule_interval=None,
    catchup=False,
    tags=['data-quality', 'foundation', 'test']
)

# Task 1: Test GE basic functionality
test_ge_task = PythonOperator(
    task_id='test_great_expectations',
    python_callable=test_great_expectations_basic,
    dag=dag
)

# Task 2: Test BigQuery provider
test_bq_task = PythonOperator(
    task_id='test_bigquery_provider',
    python_callable=test_bigquery_connection,
    dag=dag
)

# Set task dependencies
test_ge_task >> test_bq_task