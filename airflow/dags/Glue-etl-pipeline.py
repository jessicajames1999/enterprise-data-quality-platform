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
    
    message = "Glue ETL Pipeline completed successfully. Please validate the data quality  and investigate any anomalies."
    
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
        "https://app.chicory.ai/api/v1/projects/0d8bdfd5-79d5-470a-893c-35fbf2796ff3/runs",
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
    'pharma-etl-pipeline',
    default_args={
        'owner': 'data-team',
        'start_date': datetime(2024, 1, 1),
        'retries': 1,
        'retry_delay': timedelta(minutes=2)
    },
    description='Run 5 Pharma ETL jobs then notify agent for validation',
    schedule_interval=None,
    catchup=False,
    tags=['glue', 'etl', 'pharma']
)

# Job 1: Product Mastering
job1_product_mastering = GlueJobOperator(
    task_id='job1_product_mastering',
    job_name='Product-Mastering',
    region_name='us-west-2',
    aws_conn_id='aws_default',
    wait_for_completion=True,
    dag=dag
)

# Job 2: HCP to Brick
job2_hcp_brick = GlueJobOperator(
    task_id='job2_hcp_brick',
    job_name='HCP-Brick',
    region_name='us-west-2',
    aws_conn_id='aws_default',
    wait_for_completion=True,
    dag=dag
)

# Job 3: Brick to Territory
job3_brick_territory = GlueJobOperator(
    task_id='job3_brick_territory',
    job_name='Brick-Territory',
    region_name='us-west-2',
    aws_conn_id='aws_default',
    wait_for_completion=True,
    dag=dag
)

# Job 4: Sales Enrichment (depends on Jobs 1, 2, 3)
job4_sales_enrichment = GlueJobOperator(
    task_id='job4_sales_enrichment',
    job_name='Sales-Enrichment',
    region_name='us-west-2',
    aws_conn_id='aws_default',
    wait_for_completion=True,
    dag=dag
)

# Job 5: Beta Layer Validation (depends on Job 4)
job5_beta_layer = GlueJobOperator(
    task_id='job5_beta_layer',
    job_name='Beta-Layer',
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
# Job 1 (Product Mastering) ──┐
# Job 2 (HCP Brick) ──────────┼──> Job 4 (Sales Enrichment) ──> Job 5 (Beta Layer) ──> Notify Agent
# Job 3 (Brick Territory) ────┘

job1_product_mastering >> job4_sales_enrichment
job2_hcp_brick >> job4_sales_enrichment
job3_brick_territory >> job4_sales_enrichment
job4_sales_enrichment >> job5_beta_layer
job5_beta_layer >> notify_agent
