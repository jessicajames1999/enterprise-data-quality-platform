from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

def validate_customer_table():
    """Simple customer table validation - just 2 tests"""
    
    hook = BigQueryHook(gcp_conn_id='bigquery_default')
    client = hook.get_client()
    
    # Test 1: Check table has data
    query1 = "SELECT COUNT(*) as row_count FROM `chicory-mds.raw_financial_data.customer`"
    result1 = client.query(query1).to_dataframe()
    row_count = int(result1.iloc[0]['row_count'])
    
    print(f"Test 1 - Row count: {row_count}")
    test1_passed = row_count > 0
    print(f"Result: {'PASSED' if test1_passed else 'FAILED'}")
    
    # Test 2: Check ID field has no nulls
    query2 = "SELECT COUNT(*) as null_ids FROM `chicory-mds.raw_financial_data.customer` WHERE id IS NULL"
    result2 = client.query(query2).to_dataframe()
    null_ids = int(result2.iloc[0]['null_ids'])
    
    print(f"Test 2 - Null IDs: {null_ids}")
    test2_passed = null_ids == 0
    print(f"Result: {'PASSED' if test2_passed else 'FAILED'}")
    
    # Overall result
    if test1_passed and test2_passed:
        print("Overall: SUCCESS")
        return "Validation passed"
    else:
        print("Overall: FAILED")
        raise ValueError("Validation failed")

dag = DAG(
    'simple_customer_validation',
    default_args={'owner': 'data-team', 'start_date': datetime(2024, 1, 1)},
    description='Simple customer validation - 2 tests only',
    schedule_interval=None,
    catchup=False
)

validate_task = PythonOperator(
    task_id='validate_customer',
    python_callable=validate_customer_table,
    dag=dag
)