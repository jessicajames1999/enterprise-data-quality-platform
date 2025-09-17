from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

def validate_raw_tables_with_ge():
    """Validate raw AdventureWorks tables using Great Expectations with proper auth"""
    
    import great_expectations as gx
    import os
    from sqlalchemy import create_engine
    
    print("=== Great Expectations Raw Data Validation ===")
    
    # Change to GE directory
    os.chdir('/opt/airflow/great_expectations')
    
    # Get BigQuery credentials from Airflow connection
    hook = BigQueryHook(gcp_conn_id='bigquery_default')
    credentials = hook.get_credentials()
    project_id = hook.project_id or "chicory-mds"
    
    print(f"Using BigQuery project: {project_id}")
    
    # Create SQLAlchemy engine with proper credentials
    from google.cloud import bigquery
    client = bigquery.Client(credentials=credentials, project=project_id)
    
    # Build connection string with credentials
    connection_string = f"bigquery://{project_id}/raw_adventureworks"
    
    # Get GE context
    context = gx.get_context()
    print("Great Expectations context initialized")
    
    # Configure BigQuery datasource with proper credentials
    datasource_config = {
        "name": "bigquery_raw_datasource",
        "class_name": "Datasource",
        "execution_engine": {
            "class_name": "SqlAlchemyExecutionEngine",
            "connection_string": connection_string,
            "create_engine_kwargs": {
                "credentials": credentials
            }
        },
        "data_connectors": {
            "default_runtime_data_connector": {
                "class_name": "RuntimeDataConnector",
                "batch_identifiers": ["default_identifier_name"]
            }
        }
    }
    
    # Add or get datasource
    try:
        datasource = context.get_datasource("bigquery_raw_datasource")
        print("Using existing BigQuery datasource")
    except:
        context.add_datasource(**datasource_config)
        datasource = context.get_datasource("bigquery_raw_datasource")
        print("Created new BigQuery datasource")
    
    # Create or get expectation suite
    suite_name = "raw_adventureworks_validation"
    try:
        expectation_suite = context.get_expectation_suite(suite_name)
        print(f"Using existing expectation suite: {suite_name}")
    except:
        expectation_suite = context.create_expectation_suite(suite_name)
        print(f"Created new expectation suite: {suite_name}")
    
    # Test 1: Validate dim_region table
    print("\n--- GE Validation: dim_region ---")
    
    region_batch_request = {
        "datasource_name": "bigquery_raw_datasource",
        "data_connector_name": "default_runtime_data_connector",
        "data_asset_name": "dim_region",
        "runtime_parameters": {
            "query": "SELECT * FROM `chicory-mds.raw_adventureworks.dim_region`"
        },
        "batch_identifiers": {"default_identifier_name": "region_validation"}
    }
    
    region_validator = context.get_validator(
        batch_request=region_batch_request,
        expectation_suite_name=suite_name
    )
    
    # GE Expectation 1: Table should have data
    region_result1 = region_validator.expect_table_row_count_to_be_between(min_value=1)
    print(f"Region row count test: {'PASSED' if region_result1.success else 'FAILED'}")
    if region_result1.success:
        print(f"  Found {region_result1.result['observed_value']} rows")
    
    # GE Expectation 2: SalesTerritoryKey should not be null
    region_result2 = region_validator.expect_column_values_to_not_be_null(column="SalesTerritoryKey")
    print(f"Region null key test: {'PASSED' if region_result2.success else 'FAILED'}")
    if region_result2.success:
        print(f"  Null percentage: {region_result2.result['unexpected_percent']}%")
    
    # Test 2: Validate dim_salesperson table
    print("\n--- GE Validation: dim_salesperson ---")
    
    salesperson_batch_request = {
        "datasource_name": "bigquery_raw_datasource",
        "data_connector_name": "default_runtime_data_connector",
        "data_asset_name": "dim_salesperson",
        "runtime_parameters": {
            "query": "SELECT * FROM `chicory-mds.raw_adventureworks.dim_salesperson`"
        },
        "batch_identifiers": {"default_identifier_name": "salesperson_validation"}
    }
    
    salesperson_validator = context.get_validator(
        batch_request=salesperson_batch_request,
        expectation_suite_name=suite_name
    )
    
    # GE Expectation 3: Table should have data
    salesperson_result1 = salesperson_validator.expect_table_row_count_to_be_between(min_value=1)
    print(f"Salesperson row count test: {'PASSED' if salesperson_result1.success else 'FAILED'}")
    if salesperson_result1.success:
        print(f"  Found {salesperson_result1.result['observed_value']} rows")
    
    # GE Expectation 4: EmployeeKey should not be null
    salesperson_result2 = salesperson_validator.expect_column_values_to_not_be_null(column="EmployeeKey")
    print(f"Salesperson null key test: {'PASSED' if salesperson_result2.success else 'FAILED'}")
    if salesperson_result2.success:
        print(f"  Null percentage: {salesperson_result2.result['unexpected_percent']}%")
    
    # Save expectation suites
    region_validator.save_expectation_suite(discard_failed_expectations=False)
    salesperson_validator.save_expectation_suite(discard_failed_expectations=False)
    
    # Overall validation result
    all_tests = [region_result1, region_result2, salesperson_result1, salesperson_result2]
    passed_tests = [test for test in all_tests if test.success]
    
    print(f"\n=== Great Expectations Validation Summary ===")
    print(f"Tests passed: {len(passed_tests)}/{len(all_tests)}")
    
    if len(passed_tests) == len(all_tests):
        print("All Great Expectations validations PASSED")
        return "GE raw data validation successful"
    else:
        failed_tests = [test for test in all_tests if not test.success]
        print(f"Failed expectations: {len(failed_tests)}")
        raise ValueError("Great Expectations raw data validation failed")

# DAG definition
dag = DAG(
    'test_ge_bq_fixed_auth',
    default_args={
        'owner': 'data-team',
        'start_date': datetime(2024, 1, 1),
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='Test GE with fixed BigQuery authentication',
    schedule_interval=None,
    catchup=False,
    tags=['great-expectations', 'bigquery', 'fixed-auth']
)

# Single task: Test GE with proper auth
test_ge_validation = PythonOperator(
    task_id='test_ge_fixed_auth',
    python_callable=validate_raw_tables_with_ge,
    dag=dag
)