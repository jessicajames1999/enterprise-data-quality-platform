from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import requests

def notify_agent_pipeline_complete(**context):
    """Send pipeline completion to Chicory agent"""
    
    agent_token = Variable.get("api_token_anomaly")
    agent_name = Variable.get("agentid_anomaly")
    
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
        "https://app.chicory.ai/api/v1/projects/a19ef3ec-cd8f-4fd0-8440-085454810c6b/runs",
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

# Notify agent after job completes
notify_agent = PythonOperator(
    task_id='notify_agent',
    python_callable=notify_agent_pipeline_complete,
    dag=dag
)

# Pipeline flow
job1_enriched_sales >> notify_agent