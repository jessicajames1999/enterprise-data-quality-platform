from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
import time

def validate_raw_tables():
    """Validate raw AdventureWorks tables before transformation"""
    
    hook = BigQueryHook(gcp_conn_id='bigquery_default')
    client = hook.get_client()
    
    print("=== Pre-dbt Raw Data Validation ===")
    
    # Test region table
    region_count = int(client.query("SELECT COUNT(*) as count FROM `chicory-mds.raw_adventureworks.dim_region`").to_dataframe().iloc[0]['count'])
    region_nulls = int(client.query("SELECT COUNT(*) as count FROM `chicory-mds.raw_adventureworks.dim_region` WHERE SalesTerritoryKey IS NULL").to_dataframe().iloc[0]['count'])
    
    print(f"Region table: {region_count} rows, {region_nulls} null keys")
    region_passed = region_count > 0 and region_nulls == 0
    
    # Test salesperson table
    salesperson_count = int(client.query("SELECT COUNT(*) as count FROM `chicory-mds.raw_adventureworks.dim_salesperson`").to_dataframe().iloc[0]['count'])
    salesperson_nulls = int(client.query("SELECT COUNT(*) as count FROM `chicory-mds.raw_adventureworks.dim_salesperson` WHERE EmployeeKey IS NULL").to_dataframe().iloc[0]['count'])
    
    print(f"Salesperson table: {salesperson_count} rows, {salesperson_nulls} null keys")
    salesperson_passed = salesperson_count > 0 and salesperson_nulls == 0
    
    if region_passed and salesperson_passed:
        print("✅ Raw data validation PASSED - proceeding to dbt")
        return "Pre-dbt validation successful"
    else:
        raise ValueError("❌ Raw data validation failed")

def wait_for_dbt_completion(**context):
    """Wait for dbt job to complete"""
    print("⏳ Waiting for dbt job to complete...")
    time.sleep(120)  # Wait 2 minutes - adjust based on your job duration
    print("✅ dbt job wait period completed")
    return "dbt job finished"

def validate_transformed_tables():
    """Validate the 3 specific transformed tables after dbt runs"""
    
    hook = BigQueryHook(gcp_conn_id='bigquery_default')
    client = hook.get_client()
    
    print("=== Post-dbt Transformed Data Validation ===")
    
    validation_results = []
    
    # Test 1: stg_territory table
    print("\n--- stg_territory validation ---")
    try:
        stg_territory_count = int(client.query("SELECT COUNT(*) as count FROM `chicory-mds.chicory_mds_dbt_staging.stg_territory`").to_dataframe().iloc[0]['count'])
        stg_territory_nulls = int(client.query("SELECT COUNT(*) as count FROM `chicory-mds.chicory_mds_dbt_staging.stg_territory` WHERE territory_key IS NULL").to_dataframe().iloc[0]['count'])
        
        print(f"stg_territory: {stg_territory_count} rows, {stg_territory_nulls} null keys")
        territory_passed = stg_territory_count > 0 and stg_territory_nulls == 0
        validation_results.append(("stg_territory", territory_passed))
        
    except Exception as e:
        print(f"❌ stg_territory validation failed: {e}")
        validation_results.append(("stg_territory", False))
    
    # Test 2: stg_salesperson table
    print("\n--- stg_salesperson validation ---")
    try:
        stg_salesperson_count = int(client.query("SELECT COUNT(*) as count FROM `chicory-mds.chicory_mds_dbt_staging.stg_salesperson`").to_dataframe().iloc[0]['count'])
        stg_salesperson_nulls = int(client.query("SELECT COUNT(*) as count FROM `chicory-mds.chicory_mds_dbt_staging.stg_salesperson` WHERE employee_key IS NULL").to_dataframe().iloc[0]['count'])
        
        print(f"stg_salesperson: {stg_salesperson_count} rows, {stg_salesperson_nulls} null keys")
        salesperson_passed = stg_salesperson_count > 0 and stg_salesperson_nulls == 0
        validation_results.append(("stg_salesperson", salesperson_passed))
        
    except Exception as e:
        print(f"❌ stg_salesperson validation failed: {e}")
        validation_results.append(("stg_salesperson", False))
    
    # Test 3: sales_performance marts table
    print("\n--- sales_performance validation ---")
    try:
        sales_performance_count = int(client.query("SELECT COUNT(*) as count FROM `chicory-mds.chicory_mds_dbt_marts.sales_performance`").to_dataframe().iloc[0]['count'])
        sales_performance_nulls = int(client.query("SELECT COUNT(*) as count FROM `chicory-mds.chicory_mds_dbt_marts.sales_performance` WHERE territory_key IS NULL OR employee_key IS NULL").to_dataframe().iloc[0]['count'])
        
        print(f"sales_performance: {sales_performance_count} rows, {sales_performance_nulls} null keys")
        performance_passed = sales_performance_count > 0 and sales_performance_nulls == 0
        validation_results.append(("sales_performance", performance_passed))
        
    except Exception as e:
        print(f"❌ sales_performance validation failed: {e}")
        validation_results.append(("sales_performance", False))
    
    # Overall results
    print(f"\n=== Validation Summary ===")
    passed_count = 0
    for table_name, passed in validation_results:
        status = "✅ PASSED" if passed else "❌ FAILED"
        print(f"{table_name}: {status}")
        if passed:
            passed_count += 1
    
    overall_passed = passed_count == len(validation_results)
    print(f"\nOverall: {passed_count}/{len(validation_results)} tests passed")
    
    if overall_passed:
        print("🎉 All post-dbt validations PASSED - pipeline complete!")
        return "Post-dbt validation successful"
    else:
        raise ValueError(f"❌ Post-dbt validation failed - only {passed_count}/{len(validation_results)} tests passed")

# DAG definition
dag = DAG(
    'adventureworks_bq_to_dbt_pipeline',
    default_args={
        'owner': 'data-team',
        'start_date': datetime(2024, 1, 1),
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='Complete pipeline: BQ validation → dbt → post-dbt validation (territory, salesperson, sales_performance)',
    schedule_interval=None,
    catchup=False,
    tags=['adventureworks', 'complete-pipeline', 'dbt', 'staging', 'marts']
)

# Task 1: Validate raw data
validate_raw = PythonOperator(
    task_id='validate_raw_data',
    python_callable=validate_raw_tables,
    dag=dag
)

# Task 2: Trigger dbt job
trigger_dbt = SimpleHttpOperator(
    task_id='trigger_dbt_job',
    http_conn_id='dbt_cloud_default',
    endpoint='/api/v2/accounts/262085/jobs/921026/run/',
    method='POST',
    headers={'Authorization': 'Token {{ conn.dbt_cloud_default.password }}'},
    dag=dag
)

# Task 3: Wait for dbt completion
wait_dbt = PythonOperator(
    task_id='wait_for_dbt',
    python_callable=wait_for_dbt_completion,
    dag=dag
)

# Task 4: Validate transformed data
validate_transformed = PythonOperator(
    task_id='validate_transformed_data',
    python_callable=validate_transformed_tables,
    dag=dag
)

# Pipeline flow
validate_raw >> trigger_dbt >> wait_dbt >> validate_transformed