"""DAG: Hello World EMR Spark.

Uploads a PySpark hello world script to S3, then runs it as an
EMR step via EmrAddStepsOperator and waits for completion with
EmrStepSensor. Requires AWS credentials and an active EMR cluster.
"""

import os
from datetime import datetime

from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.amazon.aws.transfers.local_to_s3 import (
    LocalFilesystemToS3Operator,
)

S3_LANDING_BUCKET = os.environ.get("S3_LANDING_BUCKET", "data-processing-landing")
EMR_CLUSTER_ID = os.environ.get("EMR_CLUSTER_ID", "")
SCRIPT_S3_KEY = "scripts/hello_world_emr.py"
SCRIPT_LOCAL_PATH = "/opt/airflow/dags/spark_scripts/hello_world_emr.py"

SPARK_STEP = [
    {
        "Name": "HelloWorldEMR",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode", "cluster",
                f"s3://{S3_LANDING_BUCKET}/{SCRIPT_S3_KEY}",
            ],
        },
    }
]

with DAG(
    dag_id="hello_world_emr_spark",
    description="Hello world PySpark job on AWS EMR",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["spark", "emr", "aws", "hello-world"],
) as dag:

    upload_script = LocalFilesystemToS3Operator(
        task_id="upload_script_to_s3",
        filename=SCRIPT_LOCAL_PATH,
        dest_key=f"s3://{S3_LANDING_BUCKET}/{SCRIPT_S3_KEY}",
        aws_conn_id="aws_default",
        replace=True,
    )

    add_step = EmrAddStepsOperator(
        task_id="add_emr_step",
        job_flow_id=EMR_CLUSTER_ID,
        steps=SPARK_STEP,
        aws_conn_id="aws_default",
    )

    wait_for_step = EmrStepSensor(
        task_id="wait_for_emr_step",
        job_flow_id=EMR_CLUSTER_ID,
        step_id="{{ task_instance.xcom_pull(task_ids='add_emr_step', key='return_value')[0] }}",
        aws_conn_id="aws_default",
        poke_interval=30,
        timeout=600,
    )

    upload_script >> add_step >> wait_for_step
