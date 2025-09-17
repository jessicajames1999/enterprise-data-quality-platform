from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

def validate_with_ge_pandas():
    """Use real GE on pandas data from BigQuery"""
    
    import great_expectations as gx
    import os
    
    print("=== Great Expectations Pandas Approach ===")
    
    # Get data using Airflow's proven BigQuery connection
    hook = BigQueryHook(gcp_conn_id='bigquery_default')
    
    # Query region data
    print("Fetching dim_region data...")
    region_df = hook.get_pandas_df("SELECT * FROM `chicory-mds.raw_adventureworks.dim_region`")
    print(f"Retrieved {len(region_df)} rows from dim_region")
    
    # Query salesperson data  
    print("Fetching dim_salesperson data...")
    salesperson_df = hook.get_pandas_df("SELECT * FROM `chicory-mds.raw_adventureworks.dim_salesperson`")
    print(f"Retrieved {len(salesperson_df)} rows from dim_salesperson")
    
    # Initialize GE context (in-memory, no file system issues)
    context = gx.get_context()
    print("Great Expectations context initialized")
    
    # Test 1: Validate region DataFrame
    print("\n--- GE Validation: dim_region DataFrame ---")
    
    region_batch = context.sources.pandas_default.read_dataframe(region_df)
    
    # Real GE expectations
    region_result1 = region_batch.expect_table_row_count_to_be_between(min_value=1)
    print(f"Region row count: {'PASSED' if region_result1.success else 'FAILED'}")
    print(f"  Found {region_result1.result['observed_value']} rows")
    
    region_result2 = region_batch.expect_column_values_to_not_be_null(column="SalesTerritoryKey")
    print(f"Region null check: {'PASSED' if region_result2.success else 'FAILED'}")
    print(f"  Null percentage: {region_result2.result['unexpected_percent']:.2f}%")
    
    # Test 2: Validate salesperson DataFrame
    print("\n--- GE Validation: dim_salesperson DataFrame ---")
    
    salesperson_batch = context.sources.pandas_default.read_dataframe(salesperson_df)
    
    salesperson_result1 = salesperson_batch.expect_table_row_count_to_be_between(min_value=1)
    print(f"Salesperson row count: {'PASSED' if salesperson_result1.success else 'FAILED'}")
    print(f"  Found {salesperson_result1.result['observed_value']} rows")
    
    salesperson_result2 = salesperson_batch.expect_column_values_to_not_be_null(column="EmployeeKey")
    print(f"Salesperson null check: {'PASSED' if salesperson_result2.success else 'FAILED'}")
    print(f"  Null percentage: {salesperson_result2.result['unexpected_percent']:.2f}%")
    
    # Overall results
    all_results = [region_result1, region_result2, salesperson_result1, salesperson_result2]
    passed_count = sum(1 for r in all_results if r.success)
    
    print(f"\n=== GE Validation Summary ===")
    print(f"Tests passed: {passed_count}/{len(all_results)}")
    print("Great Expectations framework: SUCCESS")
    
    if passed_count == len(all_results):
        print("All GE validations PASSED!")
        return "GE pandas validation successful"
    else:
        raise ValueError(f"GE validation failed: {len(all_results) - passed_count} tests failed")

# DAG definition
dag = DAG(
    'test_ge_pandas_approach',
    default_args={
        'owner': 'data-team',
        'start_date': datetime(2024, 1, 1),
        'retries': 1
    },
    description='Test GE with pandas DataFrames',
    schedule_interval=None,
    catchup=False
)

test_ge_pandas = PythonOperator(
    task_id='test_ge_pandas',
    python_callable=validate_with_ge_pandas,
    dag=dag
)