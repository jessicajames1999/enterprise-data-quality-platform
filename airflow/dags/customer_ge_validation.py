from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import great_expectations as gx
import os
import yaml

def validate_customer_table():
    """Validate customer table with Great Expectations"""
    
    # Change to GE directory
    os.chdir('/opt/airflow/great_expectations')
    
    # Get GE context
    context = gx.get_context()
    
    # Define BigQuery datasource configuration
    datasource_config = {
        "name": "bigquery_datasource",
        "class_name": "Datasource",
        "execution_engine": {
            "class_name": "SqlAlchemyExecutionEngine",
            "connection_string": "bigquery://chicory-mds/raw_financial_data"
        },
        "data_connectors": {
            "default_runtime_data_connector": {
                "class_name": "RuntimeDataConnector",
                "batch_identifiers": ["default_identifier_name"]
            }
        }
    }
    
    # Add datasource
    try:
        datasource = context.get_datasource("bigquery_datasource")
        print("Using existing BigQuery datasource")
    except:
        context.add_datasource(**datasource_config)
        datasource = context.get_datasource("bigquery_datasource")
        print("Created new BigQuery datasource")
    
    # Create data asset for customer table
    asset_name = "customer_table"
    try:
        data_asset = datasource.get_asset(asset_name)
        print(f"Using existing data asset: {asset_name}")
    except:
        data_asset = datasource.add_query_asset(
            name=asset_name,
            query="SELECT * FROM `chicory-mds.raw_financial_data.customer`"
        )
        print(f"Created new data asset: {asset_name}")
    
    # Create batch request
    batch_request = data_asset.build_batch_request()
    
    # Create or get expectation suite
    suite_name = "customer_validation_suite"
    try:
        expectation_suite = context.get_expectation_suite(suite_name)
        print(f"Using existing expectation suite: {suite_name}")
    except:
        expectation_suite = context.create_expectation_suite(suite_name)
        print(f"Created new expectation suite: {suite_name}")
    
    # Get validator
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite_name
    )
    
    # Run validations
    print("Starting customer table validations...")
    
    # Test 1: Table has data
    print("Test 1: Checking if table has data...")
    result1 = validator.expect_table_row_count_to_be_between(min_value=1)
    print(f"Row count test: {'PASSED' if result1.success else 'FAILED'}")
    if result1.success:
        print(f"Table has {result1.result['observed_value']} rows")
    
    # Test 2: Customer ID column exists
    print("Test 2: Checking if customer_id column exists...")
    result2 = validator.expect_column_to_exist(column="customer_id")
    print(f"Column exists test: {'PASSED' if result2.success else 'FAILED'}")
    
    # Test 3: Customer ID has no nulls
    print("Test 3: Checking customer_id for null values...")
    result3 = validator.expect_column_values_to_not_be_null(column="customer_id")
    print(f"No null values test: {'PASSED' if result3.success else 'FAILED'}")
    if result3.success:
        print(f"Null percentage: {result3.result['unexpected_percent']}%")
    
    # Save expectation suite
    validator.save_expectation_suite(discard_failed_expectations=False)
    
    # Create checkpoint
    checkpoint_name = "customer_checkpoint"
    checkpoint_config = {
        "name": checkpoint_name,
        "config_version": 1.0,
        "template_name": None,
        "module_name": "great_expectations.checkpoint",
        "class_name": "Checkpoint",
        "run_name_template": "%Y%m%d-%H%M%S-customer-validation",
        "expectation_suite_name": suite_name,
        "batch_request": batch_request,
        "action_list": [
            {
                "name": "store_validation_result",
                "action": {"class_name": "StoreValidationResultAction"},
            }
        ],
    }
    
    try:
        context.add_checkpoint(**checkpoint_config)
        print(f"Created checkpoint: {checkpoint_name}")
    except:
        print(f"Checkpoint {checkpoint_name} already exists")
    
    # Run checkpoint
    print("Running validation checkpoint...")
    results = context.run_checkpoint(checkpoint_name=checkpoint_name)
    
    # Evaluate results
    validation_success = results["success"]
    print(f"Overall validation result: {'SUCCESS' if validation_success else 'FAILURE'}")
    
    if validation_success:
        print("All customer table validations passed!")
        return "Customer data quality validation successful"
    else:
        print("Some customer table validations failed!")
        failed_expectations = []
        for result in results["run_results"]:
            for validation_result in results["run_results"][result]["validation_result"]["results"]:
                if not validation_result["success"]:
                    failed_expectations.append(validation_result["expectation_config"]["expectation_type"])
        
        print(f"Failed expectations: {failed_expectations}")
        raise ValueError(f"Customer data quality validation failed: {failed_expectations}")

# DAG definition
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'customer_ge_validation',
    default_args=default_args,
    description='Phase 1: Great Expectations validation for customer table',
    schedule_interval=None,  # Manual trigger for testing
    catchup=False,
    tags=['great-expectations', 'bigquery', 'customer', 'phase-1']
)

# Single task for now
validate_customer_task = PythonOperator(
    task_id='validate_customer_data',
    python_callable=validate_customer_table,
    dag=dag
)