The repository contains spark applications and various ways to deploy and run data pipelines.
AWS Deployment is under aws_deployment folder, it has a cloudformation template, code for specific to AWS, and deployment code specific to AWS.
Local Spark Deployment is under local_spark_deployment, contains dockerfile to deploy spark cluster locally user and it connects to the airflow cluster mentioned above
Airflow Deployment is under airflow_deployment, it contains a dockerfile to deploy airflow locally using docker, shell scripts to create it, a shell script to deploy dag bundles which contains workflows that contain spark applications. The Spark Applications are run in the AWS environment mentioned above or in the local spark cluster mentioned above
The actual Spark Applications are under spark_applications and are in pyspark.
Databricks Deployment should contain databricks specific databricks infrastructure code and deployment to databricks code
Under web_server_code should contain web server code using FastAPI that serves file based on parameters
Under web_server_local should contain code to deploy web server locally
Under web_server_aws should contain code to deploy web server to AWS mentioned in the beginning
