from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

def validate_region_and_salesperson():
    """Simple validation for dim_region and dim_salesperson tables"""
    
    hook = BigQueryHook(gcp_conn_id='bigquery_default')
    client = hook.get_client()
    
    print("=== AdventureWorks Region & Salesperson Validation ===")
    
    # Test 1: Region table validation
    print("\n--- dim_region Table ---")
    region_count_query = "SELECT COUNT(*) as count FROM `chicory-mds.raw_adventureworks.dim_region`"
    region_null_query = "SELECT COUNT(*) as count FROM `chicory-mds.raw_adventureworks.dim_region` WHERE RegionID IS NULL"
    
    region_count = int(client.query(region_count_query).to_dataframe().iloc[0]['count'])
    region_nulls = int(client.query(region_null_query).to_dataframe().iloc[0]['count'])
    
    print(f"Row count: {region_count}")
    print(f"Null RegionIDs: {region_nulls}")
    
    region_test_passed = region_count > 0 and region_nulls == 0
    print(f"Region validation: {'PASSED' if region_test_passed else 'FAILED'}")
    
    # Test 2: Salesperson table validation  
    print("\n--- dim_salesperson Table ---")
    salesperson_count_query = "SELECT COUNT(*) as count FROM `chicory-mds.raw_adventureworks.dim_salesperson`"
    salesperson_null_query = "SELECT COUNT(*) as count FROM `chicory-mds.raw_adventureworks.dim_salesperson` WHERE SalespersonID IS NULL"
    
    salesperson_count = int(client.query(salesperson_count_query).to_dataframe().iloc[0]['count'])
    salesperson_nulls = int(client.query(salesperson_null_query).to_dataframe().iloc[0]['count'])
    
    print(f"Row count: {salesperson_count}")
    print(f"Null SalespersonIDs: {salesperson_nulls}")
    
    salesperson_test_passed = salesperson_count > 0 and salesperson_nulls == 0
    print(f"Salesperson validation: {'PASSED' if salesperson_test_passed else 'FAILED'}")
    
    # Overall result
    all_tests_passed = region_test_passed and salesperson_test_passed
    print(f"\nOverall Result: {'SUCCESS' if all_tests_passed else 'FAILURE'}")
    
    if all_tests_passed:
        return "Region and Salesperson validation passed - ready for dbt"
    else:
        raise ValueError("Some validations failed - stopping pipeline")

dag = DAG(
    'adventureworks_region_salesperson_validation',
    default_args={'owner': 'data-team', 'start_date': datetime(2024, 1, 1)},
    description='Validate AdventureWorks region and salesperson tables',
    schedule_interval=None,
    catchup=False,
    tags=['adventureworks', 'region', 'salesperson', 'validation']
)

validate_task = PythonOperator(
    task_id='validate_region_salesperson',
    python_callable=validate_region_and_salesperson,
    dag=dag
)

# We'll add the dbt trigger task once you provide the job details