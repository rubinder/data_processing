"""DAG: Hello World Local Spark.

Submits a PySpark hello world script to a local Spark cluster
via SparkSubmitOperator. Requires local_spark_deployment running
on the data-processing-network Docker network.
"""

from datetime import datetime

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import (
    SparkSubmitOperator,
)

with DAG(
    dag_id="hello_world_local_spark",
    description="Hello world PySpark job on local Spark cluster",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["spark", "local", "hello-world"],
) as dag:

    submit_spark_job = SparkSubmitOperator(
        task_id="submit_hello_world_local",
        conn_id="spark_local",
        application="/opt/airflow/dags/spark_scripts/hello_world_local.py",
        name="hello_world_local",
        verbose=True,
    )
