from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
import json

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
        print("Raw data validation PASSED - proceeding to dbt")
        return "Pre-dbt validation successful"
    else:
        raise ValueError("Raw data validation failed")

def validate_transformed_tables():
    """Validate transformed tables after dbt runs - with region validation that will fail"""
    
    hook = BigQueryHook(gcp_conn_id='bigquery_default')
    client = hook.get_client()
    
    print("=== Post-dbt Transformed Data Validation ===")
    
    validation_results = []
    
    # Test 1: stg_territory table
    print("\n--- stg_territory validation ---")
    try:
        stg_territory_count = int(client.query("SELECT COUNT(*) as count FROM `chicory-mds.chicory_mds_staging.stg_territory`").to_dataframe().iloc[0]['count'])
        print(f"stg_territory: {stg_territory_count} rows")
        territory_passed = stg_territory_count > 0
        validation_results.append(("stg_territory", territory_passed))
        
    except Exception as e:
        print(f"stg_territory validation failed: {e}")
        validation_results.append(("stg_territory", False))
    
    # Test 2: stg_salesperson table
    print("\n--- stg_salesperson validation ---")
    try:
        stg_salesperson_count = int(client.query("SELECT COUNT(*) as count FROM `chicory-mds.chicory_mds_staging.stg_salesperson`").to_dataframe().iloc[0]['count'])
        print(f"stg_salesperson: {stg_salesperson_count} rows")
        salesperson_passed = stg_salesperson_count > 0
        validation_results.append(("stg_salesperson", salesperson_passed))
        
    except Exception as e:
        print(f"stg_salesperson validation failed: {e}")
        validation_results.append(("stg_salesperson", False))
    
    # Test 3: sales_performance marts table
    print("\n--- sales_performance validation ---")
    try:
        sales_performance_count = int(client.query("SELECT COUNT(*) as count FROM `chicory-mds.chicory_mds_marts.mart_sales_perfromance_dashboard`").to_dataframe().iloc[0]['count'])
        print(f"sales_performance: {sales_performance_count} rows")
        performance_passed = sales_performance_count > 0
        validation_results.append(("sales_performance", performance_passed))
        
    except Exception as e:
        print(f"sales_performance validation failed: {e}")
        validation_results.append(("sales_performance", False))
    
    # NEW Test 4: Region whitelist validation (this will fail because South America/Brazil is excluded)
    print("\n--- region_whitelist validation ---")
    try:
        # Define allowed regions (excluding South America to create failure)
        allowed_regions = [
            'Northwest', 'Northeast', 'Central', 'Southwest', 'Southeast',
            'Canada', 'France', 'Germany', 'Australia', 'United Kingdom'
            # Note: 'South America' is intentionally excluded to trigger failure
        ]
        
        # Check if any regions in staging are not in the allowed list
        region_query = f"""
        SELECT DISTINCT Region
        FROM `chicory-mds.chicory_mds_staging.stg_territory`
        WHERE Region NOT IN ({','.join([f"'{r}'" for r in allowed_regions])})
        """
        
        invalid_regions_df = client.query(region_query).to_dataframe()
        
        if len(invalid_regions_df) > 0:
            invalid_region_names = invalid_regions_df['Region'].tolist()
            print(f"VALIDATION FAILURE: Found {len(invalid_regions_df)} unauthorized regions: {invalid_region_names}")
            print(f"Allowed regions: {allowed_regions}")
            validation_results.append(("region_whitelist", False))
        else:
            print(f"All regions are authorized")
            validation_results.append(("region_whitelist", True))
            
    except Exception as e:
        print(f"region_whitelist validation failed: {e}")
        validation_results.append(("region_whitelist", False))
    
    # Overall results
    print(f"\n=== Validation Summary ===")
    passed_count = 0
    for table_name, passed in validation_results:
        status = "PASSED" if passed else "FAILED"
        print(f"{table_name}: {status}")
        if passed:
            passed_count += 1
    
    overall_passed = passed_count == len(validation_results)
    print(f"\nOverall: {passed_count}/{len(validation_results)} tests passed")
    
    if overall_passed:
        print("All post-dbt validations PASSED - pipeline complete!")
        return "Post-dbt validation successful"
    else:
        raise ValueError(f"Post-dbt validation failed - only {passed_count}/{len(validation_results)} tests passed")

# DAG definition
dag = DAG(
    'working_bq_dbt_validation_pipeline_testing',
    default_args={
        'owner': 'data-team',
        'start_date': datetime(2024, 1, 1),
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='Working BQ validation → dbt Cloud → post-dbt validation pipeline',
    schedule_interval=None,
    catchup=False,
    tags=['working', 'dbt-cloud', 'validation', 'complete']
)

# Task 1: Validate raw data
validate_raw = PythonOperator(
    task_id='validate_raw_data',
    python_callable=validate_raw_tables,
    dag=dag
)

# Task 2: Trigger dbt job using proven method
trigger_dbt = SimpleHttpOperator(
    task_id='trigger_dbt_cloud_job',
    http_conn_id='dbt_cloud_api',
    endpoint='/api/v2/accounts/{{ var.value.dbt_account_id }}/jobs/{{ var.value.dbt_job_id }}/run/',
    method='POST',
    headers={
        'Authorization': 'Token {{ var.value.dbt_cloud_token }}',
        'Content-Type': 'application/json'
    },
    data=json.dumps({
        'cause': 'Triggered by Airflow BQ Validation Pipeline',
        'git_sha': None,
    }),
    dag=dag
)

# Task 3: Wait for dbt completion (simple wait)
wait_dbt = PythonOperator(
    task_id='wait_for_dbt',
    python_callable=lambda: __import__('time').sleep(120),  # Wait 2 minutes
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