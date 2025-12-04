from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import requests

def notify_agent_pipeline_complete(**context):
    """Send pipeline completion to Chicory agent"""
    
    agent_token = Variable.get("chicory_agent_token")
    agent_name = Variable.get("chicory_agent_name")
    
    message = "Glue ETL Pipeline completed successfully. Please analyze the final reports in s3://adventureworks-demo-gilead/gold/"
    
    agent_payload = {
        "agent_name": agent_name,
        "input": [
            {
                "parts": [
                    {
                        "content_type": "text/plain",
                        "content": message
                    }
                ],
                "created_at": datetime.now().isoformat() + "Z"
            }
        ]
    }
    
    response = requests.post(
        "https://app.chicory.ai/api/v1/projects/247190cd-998a-47f4-97ef-2189be433448/runs",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {agent_token}"
        },
        json=agent_payload,
        timeout=30
    )
    
    if response.status_code in [200, 201]:
        print("Successfully notified Chicory agent")
        print(f"Response: {response.text}")
    else:
        print(f"Failed to notify agent: {response.status_code}")
        print(f"Response: {response.text}")

# DAG definition
dag = DAG(
    'glue-etl-pipeline',
    default_args={
        'owner': 'data-team',
        'start_date': datetime(2024, 1, 1),
        'retries': 1,
        'retry_delay': timedelta(minutes=2)
    },
    description='Run 3 Glue ETL jobs then notify agent for validation',
    schedule_interval=None,
    catchup=False,
    tags=['glue', 'etl', 'adventureworks']
)

# Job 1: Enriched Sales
job1_enriched_sales = GlueJobOperator(
    task_id='job1_enriched_sales',
    job_name='Enriched-Sales',
    region_name='us-west-2',
    aws_conn_id='aws_default',
    wait_for_completion=True,
    dag=dag
)

# Job 2: Clean Customer Orders
job2_customer_orders = GlueJobOperator(
    task_id='job2_customer_orders',
    job_name='Clean-Customer-Orders',
    region_name='us-west-2',
    aws_conn_id='aws_default',
    wait_for_completion=True,
    dag=dag
)

# Job 3: Sales Summary
job3_sales_summary = GlueJobOperator(
    task_id='job3_sales_summary',
    job_name='Sales-Summary',
    region_name='us-west-2',
    aws_conn_id='aws_default',
    wait_for_completion=True,
    dag=dag
)

# Notify agent after all jobs complete
notify_agent = PythonOperator(
    task_id='notify_agent',
    python_callable=notify_agent_pipeline_complete,
    dag=dag
)

# Pipeline flow:
# Job 1 → Job 3 → Notify Agent
# Job 2 → Notify Agent
[job1_enriched_sales >> job3_sales_summary, job2_customer_orders] >> notify_agent