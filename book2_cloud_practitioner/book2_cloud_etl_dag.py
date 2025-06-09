# book2_cloud_etl_dag.py
#
# This is the final DAG from Chapter 8 of "Cloud Data Engineering for Practitioners."
# It is designed to be run in a managed environment like Amazon MWAA.
# This DAG programmatically creates an EMR cluster, submits a Spark job,
# waits for it to complete, and then automatically terminates the cluster.

from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.providers.amazon.aws.operators.emr import (
    EmrAddStepsOperator,
    EmrCreateJobFlowOperator,
    EmrTerminateJobFlowOperator,
)
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor

# --- CONFIGURATION ---
# IMPORTANT: Replace these with your specific AWS details.
S3_BUCKET = "your-unique-bucket-name"
AWS_REGION = "us-east-1" # e.g., "us-east-1"
EC2_KEY_PAIR_NAME = "" # Optional: add your EC2 key pair name if you need to SSH into the cluster
EMR_EC2_ROLE = "EMR_EC2_DefaultRole"
EMR_SERVICE_ROLE = "EMR_DefaultRole"

# Define the location of your Spark script in S3
S3_SCRIPT_PATH = f"s3://{S3_BUCKET}/spark-scripts/etl_spark_s3.py"

# EMR Cluster Configuration defined as a dictionary.
# This makes the cluster definition reusable and readable.
JOB_FLOW_OVERRIDES = {
    "Name": "Automated-Spark-ETL-Cluster-from-Airflow",
    "ReleaseLabel": "emr-6.15.0",
    "Applications": [{"Name": "Spark"}],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master node",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core nodes",
                "Market": "ON_DEMAND",
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 2,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": False, # This is key for auto-termination
        "TerminationProtected": False,
        "Ec2KeyName": EC2_KEY_PAIR_NAME,
    },
    "VisibleToAllUsers": True,
    "JobFlowRole": EMR_EC2_ROLE,
    "ServiceRole": EMR_SERVICE_ROLE,
}

# Spark Step Configuration.
# This defines the spark-submit job to be run on the cluster.
SPARK_STEP = [
    {
        "Name": "Run S3 ETL Spark Job",
        "ActionOnFailure": "CONTINUE", # Or "TERMINATE_JOB_FLOW"
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "cluster",
                S3_SCRIPT_PATH,
            ],
        },
    }
]


with DAG(
    dag_id="final_cloud_etl_pipeline",
    start_date=pendulum.datetime(2025, 6, 8, tz="UTC"),
    schedule="@daily",
    catchup=False,
    doc_md="""
    ### Final Cloud ETL Pipeline
    This DAG automates a data pipeline in AWS.
    1. Creates an EMR cluster.
    2. Submits a PySpark job to process data from S3.
    3. Automatically terminates the cluster upon completion.
    """,
    tags=["data-engineering", "cloud", "final"],
) as dag:

    # Task 1: Create an EMR cluster.
    # The 'job_flow_overrides' parameter takes our dictionary defined above.
    create_emr_cluster = EmrCreateJobFlowOperator(
        task_id="create_emr_cluster",
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id="aws_default", # Uses the MWAA Execution Role credentials
        region_name=AWS_REGION,
    )

    # Task 2: Add the Spark job as a step to the newly created cluster.
    # It pulls the cluster ID from the previous task using XComs.
    add_spark_step = EmrAddStepsOperator(
        task_id="add_spark_step",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        steps=SPARK_STEP,
    )

    # Task 3: Wait for the Spark step to complete. This is a "sensor" operator.
    # It will check the status of the step periodically until it succeeds or fails.
    wait_for_spark_step = EmrStepSensor(
        task_id="wait_for_spark_step",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_spark_step', key='return_value')[0] }}",
    )

    # The EmrTerminateJobFlowOperator is not needed because we set
    # 'KeepJobFlowAliveWhenNoSteps' to False in the cluster configuration.

    # Define task dependencies
    create_emr_cluster >> add_spark_step >> wait_for_spark_step
