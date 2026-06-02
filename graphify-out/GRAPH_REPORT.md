# Graph Report - .  (2026-06-02)

## Corpus Check
- Corpus is ~9,837 words - fits in a single context window. You may not need a graph.

## Summary
- 294 nodes · 332 edges · 37 communities detected
- Extraction: 82% EXTRACTED · 18% INFERRED · 0% AMBIGUOUS · INFERRED: 61 edges (avg confidence: 0.65)
- Token cost: 0 input · 0 output

## Community Hubs (Navigation)
- [[_COMMUNITY_Aggregation & API Pull Jobs|Aggregation & API Pull Jobs]]
- [[_COMMUNITY_Storage Adapters (AWSDatabricks)|Storage Adapters (AWS/Databricks)]]
- [[_COMMUNITY_DatabricksdbtDebeziumFlink|Databricks/dbt/Debezium/Flink]]
- [[_COMMUNITY_Generator Test Suite|Generator Test Suite]]
- [[_COMMUNITY_Airflow & Deployment Orchestration|Airflow & Deployment Orchestration]]
- [[_COMMUNITY_Storage Factory & Test Fixtures|Storage Factory & Test Fixtures]]
- [[_COMMUNITY_Local Storage & API Pull Tests|Local Storage & API Pull Tests]]
- [[_COMMUNITY_AWS Batch & Step Functions|AWS Batch & Step Functions]]
- [[_COMMUNITY_CloudFormation Deploy Script|CloudFormation Deploy Script]]
- [[_COMMUNITY_Impression Data Generator|Impression Data Generator]]
- [[_COMMUNITY_Salted Join Job|Salted Join Job]]
- [[_COMMUNITY_Flink Hello World Tests|Flink Hello World Tests]]
- [[_COMMUNITY_FastAPI Endpoint Tests|FastAPI Endpoint Tests]]
- [[_COMMUNITY_Web Server AWS Deployment|Web Server AWS Deployment]]
- [[_COMMUNITY_Salted Join Tests|Salted Join Tests]]
- [[_COMMUNITY_Flink Hello World Job|Flink Hello World Job]]
- [[_COMMUNITY_Aggregation Tests|Aggregation Tests]]
- [[_COMMUNITY_Spark Apps & Local Deploy|Spark Apps & Local Deploy]]
- [[_COMMUNITY_Spark Test Fixtures|Spark Test Fixtures]]
- [[_COMMUNITY_Lambda Step Function Trigger|Lambda Step Function Trigger]]
- [[_COMMUNITY_FastAPI Impression Server|FastAPI Impression Server]]
- [[_COMMUNITY_Salted Join Functions|Salted Join Functions]]
- [[_COMMUNITY_Local Spark Hello World|Local Spark Hello World]]
- [[_COMMUNITY_EMR Spark Hello World|EMR Spark Hello World]]
- [[_COMMUNITY_Local Spark DAG|Local Spark DAG]]
- [[_COMMUNITY_EMR Spark DAG|EMR Spark DAG]]
- [[_COMMUNITY_Glue ETL Job|Glue ETL Job]]
- [[_COMMUNITY_Package Init|Package Init]]
- [[_COMMUNITY_Package Init|Package Init]]
- [[_COMMUNITY_Package Init|Package Init]]
- [[_COMMUNITY_Package Init|Package Init]]
- [[_COMMUNITY_Package Init|Package Init]]
- [[_COMMUNITY_Package Init|Package Init]]
- [[_COMMUNITY_Package Init|Package Init]]
- [[_COMMUNITY_Package Init|Package Init]]
- [[_COMMUNITY_Project Coding Standards|Project Coding Standards]]
- [[_COMMUNITY_uv Python Toolchain|uv Python Toolchain]]

## God Nodes (most connected - your core abstractions)
1. `Mode` - 28 edges
2. `LocalStorageAdapter` - 15 edges
3. `AwsStorageAdapter` - 13 edges
4. `DatabricksStorageAdapter` - 12 edges
5. `Multi-Platform Data Processing Project` - 11 edges
6. `get_storage_adapter()` - 9 edges
7. `StorageAdapter` - 7 edges
8. `check_encoding()` - 7 edges
9. `load_impressions()` - 6 edges
10. `AWS Deployment Architecture` - 6 edges

## Surprising Connections (you probably didn't know these)
- `Debezium CDC Deployment` --semantically_similar_to--> `raw.impressions PostgreSQL table`  [INFERRED] [semantically similar]
  debezium_deployment/README.md → dbt_deployment/load_data.py
- `int_impressions_aggregated model` --semantically_similar_to--> `aggregate_impressions()`  [INFERRED] [semantically similar]
  dbt_deployment/README.md → spark_applications/spark_applications/aggregation.py
- `Glue ETL Job (CSV to Parquet)` --conceptually_related_to--> `AWS Deployment (CloudFormation)`  [INFERRED]
  aws_deployment/glue/etl_job.py → Planning.md
- `data_processing_workflow (Databricks Workflow)` --references--> `spark hello_world main()`  [INFERRED]
  databricks_deployment/README.md → spark_applications/spark_applications/hello_world.py
- `load_impressions()` --semantically_similar_to--> `api_pull main()`  [INFERRED] [semantically similar]
  dbt_deployment/load_data.py → spark_applications/spark_applications/api_pull.py

## Hyperedges (group relationships)
- **S3-Event-Driven AWS Pipeline** — lambda_trigger_handler, awsreadme_step_function, check_encoding_check_encoding, glue_etl_job [EXTRACTED 0.85]
- **Airflow Hello World Spark Workflows** — hello_world_emr_spark_dag, hello_world_local_spark_dag, hello_world_emr_script, hello_world_local_script [EXTRACTED 0.85]
- **Impression Data Domain Model** — tasks_impression_data_spec, tasks_event_funnel_spec, planning_web_server_code, planning_spark_applications [INFERRED 0.75]
- **Databricks workflow orchestrating Spark jobs** — databricks_readme_workflow, spark_api_pull_main, spark_aggregation_main, spark_hello_world_main [INFERRED 0.80]
- **Salted join data-skew mitigation pattern** — spark_salted_join_add_salt, spark_salted_join_explode, spark_salted_join_remove_salt [EXTRACTED 0.90]
- **Impression data pipeline (pull, aggregate, model)** — impression_api, spark_api_pull_main, spark_aggregation_main, dbt_readme_int_aggregated [INFERRED 0.75]
- **Mode-driven storage adapter strategy** — storage_StorageAdapter, storage_LocalStorageAdapter, storage_AwsStorageAdapter, storage_DatabricksStorageAdapter, storage_get_storage_adapter, mode_Mode [EXTRACTED 0.90]
- **Impression funnel data generation flow** — webcode_get_impression, webcode_generate_gzip_csv, webcode_generate_rows, webcode_determine_max_event, webcode_FUNNEL_RATES [EXTRACTED 0.90]
- **FastAPI web server deployment targets** — webcode_readme, webaws_readme, weblocal_readme [EXTRACTED 0.85]

## Communities

### Community 0 - "Aggregation & API Pull Jobs"
Cohesion: 0.08
Nodes (30): aggregate_impressions(), main(), parse_args(), PySpark job that aggregates impression data., Parse command line arguments., Aggregate impression data by user_id, impression_id, page_type.      Computes:, fetch_impression_data(), main() (+22 more)

### Community 1 - "Storage Adapters (AWS/Databricks)"
Cohesion: 0.08
Nodes (8): ABC, AwsStorageAdapter, DatabricksStorageAdapter, Mode-specific storage adapters for reading/writing data., Storage adapter for AWS (S3 + DynamoDB)., Abstract base class for mode-specific storage operations., Storage adapter for Databricks (Delta tables + DBFS)., StorageAdapter

### Community 2 - "Databricks/dbt/Debezium/Flink"
Cohesion: 0.09
Nodes (25): Databricks Deployment, Databricks Runtime 17.3 / job cluster choice, data_processing_workflow (Databricks Workflow), dbt Analysis Models (funnel/page/user/hourly), dbt Deployment, int_impressions_aggregated model, stg_impressions model, Debezium PostgreSQL Source Connector (+17 more)

### Community 3 - "Generator Test Suite"
Cohesion: 0.08
Nodes (23): Tests for impression data generator., page_type 2: ~30% reach d, ~10% reach e, 0% reach f., page_type 3: ~50% reach d, ~20% reach e, ~10% reach f., Generated gzip CSV decompresses to valid CSV with correct header., Every impression reaches at least event 'a'., Same inputs produce the same impression_id., Different inputs produce different impression_ids., Generated rows have the correct number of fields. (+15 more)

### Community 4 - "Airflow & Deployment Orchestration"
Cohesion: 0.13
Nodes (22): Airflow Deployment Architecture, data-processing-network Docker Network, .env Environment Variables, hello_world_emr.py Spark Script, hello_world_emr_spark DAG, hello_world_local.py Spark Script, hello_world_local_spark DAG, Airflow Deployment (+14 more)

### Community 5 - "Storage Factory & Test Fixtures"
Cohesion: 0.11
Nodes (22): spark pytest fixture, AwsStorageAdapter (S3 + DynamoDB), DatabricksStorageAdapter (Delta + DBFS), LocalStorageAdapter, StorageAdapter (abstract base), get_storage_adapter(), Factory function to get the appropriate storage adapter., Aggregation test suite (+14 more)

### Community 6 - "Local Storage & API Pull Tests"
Cohesion: 0.13
Nodes (7): LocalStorageAdapter, Storage adapter for local filesystem., Tests for api_pull job., Test saving a gzip CSV and reading it back via LocalStorageAdapter., Test status update via LocalStorageAdapter., test_local_storage_round_trip(), test_local_storage_status()

### Community 7 - "AWS Batch & Step Functions"
Cohesion: 0.33
Nodes (9): AWS Deployment Architecture, Step Function Pipeline Orchestration, check_encoding(), main(), AWS Batch job to validate file encoding is UTF-8., Download file from S3 and validate encoding.      Args:         bucket: S3 bucke, Entry point for Batch job. Reads bucket/key from env vars., Glue ETL Job (CSV to Parquet) (+1 more)

### Community 8 - "CloudFormation Deploy Script"
Cohesion: 0.27
Nodes (9): deploy_stack(), main(), parse_args(), Deploy CloudFormation stack using boto3., Entry point for deployment., Check if a CloudFormation stack exists., Create or update a CloudFormation stack and wait for completion.      Args:, Parse command line arguments. (+1 more)

### Community 9 - "Impression Data Generator"
Cohesion: 0.27
Nodes (9): _determine_max_event(), generate_gzip_csv(), generate_rows(), _make_impression_id(), Impression data generator.  Generates synthetic impression event data as gzip-co, Generate impression data and return as gzip-compressed CSV bytes., Determine the deepest event reached using a single random roll., Generate deterministic impression_id for a given combination. (+1 more)

### Community 10 - "Salted Join Job"
Cohesion: 0.31
Nodes (8): add_salt_column(), explode_for_salt(), main(), Salted join PySpark job for handling data skew., Add a random salt column and a salted key to a DataFrame.      Used on the large, Explode a small DataFrame to match all possible salt values.      Used on the sm, Remove salt and salted_key columns after join., remove_salt_columns()

### Community 11 - "Flink Hello World Tests"
Cohesion: 0.29
Nodes (3): Tests for the hello_world Flink job., Verify the hello world job completes without error., test_hello_world_runs()

### Community 12 - "FastAPI Endpoint Tests"
Cohesion: 0.29
Nodes (1): Tests for the FastAPI endpoint.

### Community 13 - "Web Server AWS Deployment"
Cohesion: 0.33
Nodes (7): CloudFormation template (web_server.yaml), ECS Fargate behind ALB, Web Server AWS Deployment, GET /impression endpoint spec, Web Server Code (FastAPI impression server), data-processing-network shared Docker network, Web Server Local (Docker) Deployment

### Community 14 - "Salted Join Tests"
Cohesion: 0.33
Nodes (1): Tests for salted_join functions.

### Community 15 - "Flink Hello World Job"
Cohesion: 0.4
Nodes (1): Hello World PyFlink job — minimal batch example.

### Community 16 - "Aggregation Tests"
Cohesion: 0.4
Nodes (3): Tests for aggregation job., Test reading from parquet and aggregating., test_aggregation_with_partitioned_data()

### Community 17 - "Spark Apps & Local Deploy"
Cohesion: 0.4
Nodes (5): Databricks deploy.sh, Local Spark Deployment, Shared vs local network mode rationale, Spark Applications, Spark utils package (mode/session/storage)

### Community 18 - "Spark Test Fixtures"
Cohesion: 0.5
Nodes (3): Shared test fixtures., Session-scoped SparkSession for tests., spark()

### Community 19 - "Lambda Step Function Trigger"
Cohesion: 0.5
Nodes (3): lambda_handler(), Lambda function triggered by S3 event to start Step Function execution., Handle S3 put event and start Step Function execution.      Args:         event:

### Community 20 - "FastAPI Impression Server"
Cohesion: 0.5
Nodes (3): get_impression(), FastAPI web server that serves impression data as gzip CSV., Generate and return impression data as a gzip-compressed CSV file.

### Community 21 - "Salted Join Functions"
Cohesion: 0.67
Nodes (3): add_salt_column(), explode_for_salt(), salted_join main()

### Community 22 - "Local Spark Hello World"
Cohesion: 0.67
Nodes (1): Hello world PySpark script for local Spark cluster.

### Community 23 - "EMR Spark Hello World"
Cohesion: 0.67
Nodes (1): Hello world PySpark script for AWS EMR.

### Community 24 - "Local Spark DAG"
Cohesion: 1.0
Nodes (1): DAG: Hello World Local Spark.  Submits a PySpark hello world script to a local S

### Community 25 - "EMR Spark DAG"
Cohesion: 1.0
Nodes (1): DAG: Hello World EMR Spark.  Uploads a PySpark hello world script to S3, then ru

### Community 26 - "Glue ETL Job"
Cohesion: 1.0
Nodes (1): AWS Glue ETL job: reads raw CSV from S3, writes partitioned parquet.

### Community 27 - "Package Init"
Cohesion: 1.0
Nodes (0): 

### Community 28 - "Package Init"
Cohesion: 1.0
Nodes (0): 

### Community 29 - "Package Init"
Cohesion: 1.0
Nodes (0): 

### Community 30 - "Package Init"
Cohesion: 1.0
Nodes (0): 

### Community 31 - "Package Init"
Cohesion: 1.0
Nodes (0): 

### Community 32 - "Package Init"
Cohesion: 1.0
Nodes (0): 

### Community 33 - "Package Init"
Cohesion: 1.0
Nodes (0): 

### Community 34 - "Package Init"
Cohesion: 1.0
Nodes (0): 

### Community 35 - "Project Coding Standards"
Cohesion: 1.0
Nodes (1): Project Coding Standards

### Community 36 - "uv Python Toolchain"
Cohesion: 1.0
Nodes (1): uv Python Toolchain

## Knowledge Gaps
- **82 isolated node(s):** `Salted join PySpark job for handling data skew.`, `Add a random salt column and a salted key to a DataFrame.      Used on the large`, `Explode a small DataFrame to match all possible salt values.      Used on the sm`, `Remove salt and salted_key columns after join.`, `Execution mode enum and CLI argument helpers.` (+77 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **Thin community `Local Spark DAG`** (2 nodes): `hello_world_local_spark.py`, `DAG: Hello World Local Spark.  Submits a PySpark hello world script to a local S`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `EMR Spark DAG`** (2 nodes): `hello_world_emr_spark.py`, `DAG: Hello World EMR Spark.  Uploads a PySpark hello world script to S3, then ru`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Glue ETL Job`** (2 nodes): `etl_job.py`, `AWS Glue ETL job: reads raw CSV from S3, writes partitioned parquet.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Package Init`** (1 nodes): `__init__.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Package Init`** (1 nodes): `__init__.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Package Init`** (1 nodes): `__init__.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Package Init`** (1 nodes): `__init__.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Package Init`** (1 nodes): `__init__.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Package Init`** (1 nodes): `__init__.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Package Init`** (1 nodes): `__init__.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Package Init`** (1 nodes): `__init__.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Project Coding Standards`** (1 nodes): `Project Coding Standards`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `uv Python Toolchain`** (1 nodes): `uv Python Toolchain`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `Mode` connect `Aggregation & API Pull Jobs` to `Storage Adapters (AWS/Databricks)`, `Storage Factory & Test Fixtures`, `Local Storage & API Pull Tests`?**
  _High betweenness centrality (0.067) - this node is a cross-community bridge._
- **Why does `get_storage_adapter()` connect `Storage Factory & Test Fixtures` to `Aggregation & API Pull Jobs`, `Storage Adapters (AWS/Databricks)`, `Local Storage & API Pull Tests`?**
  _High betweenness centrality (0.058) - this node is a cross-community bridge._
- **Are the 24 inferred relationships involving `Mode` (e.g. with `PySpark job that aggregates impression data.` and `Parse command line arguments.`) actually correct?**
  _`Mode` has 24 INFERRED edges - model-reasoned connections that need verification._
- **Are the 4 inferred relationships involving `LocalStorageAdapter` (e.g. with `Mode` and `Tests for api_pull job.`) actually correct?**
  _`LocalStorageAdapter` has 4 INFERRED edges - model-reasoned connections that need verification._
- **What connects `Salted join PySpark job for handling data skew.`, `Add a random salt column and a salted key to a DataFrame.      Used on the large`, `Explode a small DataFrame to match all possible salt values.      Used on the sm` to the rest of the system?**
  _82 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `Aggregation & API Pull Jobs` be split into smaller, more focused modules?**
  _Cohesion score 0.08 - nodes in this community are weakly interconnected._
- **Should `Storage Adapters (AWS/Databricks)` be split into smaller, more focused modules?**
  _Cohesion score 0.08 - nodes in this community are weakly interconnected._