# Databricks Deployment

Deploys Spark application workflows to a Databricks workspace.

## Architecture

How the workflow definition reaches a Databricks workspace and what its tasks run.

```mermaid
flowchart LR
    subgraph module["databricks_deployment"]
        wf["workflow.json<br/>tasks hello_world, api_pull, aggregation"]
        deploy["deploy.sh<br/>Databricks CLI, .env credentials"]
    end
    subgraph dbx["Databricks workspace"]
        job{{"Workflow job<br/>job cluster, Databricks 17.3"}}
        t1["hello_world"]
        t2["api_pull<br/>SPARK_MODE=databricks"]
        t3["aggregation"]
        delta[("DBFS delta tables<br/>impressions, pull status")]
    end
    code["spark_applications<br/>uploaded as a package"]
    api{{"web server API"}}
    deploy -->|"jobs create"| job
    wf --> deploy
    code --> job
    job --> t1
    job --> t2 --> t3
    t2 -->|"csv.gz"| api
    t2 --> delta
    delta --> t3
```

- Same Spark jobs as the local and AWS paths; only the storage adapter (delta on DBFS) and the pull-status table change.


- **workflow.json**: Defines a multi-task Databricks workflow (`data_processing_workflow`) with three tasks:
  1. `api_pull` - Pulls impression data from the API and saves to a Delta table
  2. `aggregation` - Aggregates impression data by user_id, impression_id, page_type (depends on `api_pull`)
  3. `hello_world` - Simple validation task
- Uses a job cluster with Databricks Runtime 17.3 (Spark `17.3.x-scala2.12`), 2 worker nodes (`i3.xlarge`)
- Accepts parameters: `page_type`, `date`, `hour`

- **deploy.sh**: Shell script to upload Spark application code to DBFS and manage the workflow job.

## Prerequisites

- Databricks CLI installed and configured
- A `.env` file in the project root with:
  - `DATABRICKS_HOST` - Databricks workspace URL
  - `DATABRICKS_TOKEN` - Databricks personal access token

## How to Deploy

```bash
# Full deployment: upload code + create/update the workflow job
./deploy.sh deploy

# Upload spark application files to DBFS only
./deploy.sh upload-code

# Create a new workflow job
./deploy.sh create-job

# Update an existing workflow job
./deploy.sh update-job

# Check workflow job status
./deploy.sh status

# Delete the workflow job
./deploy.sh delete-job
```

The deploy script uploads all Python files from `spark_applications/spark_applications/` (including the `utils/` package) to `dbfs:/spark_applications/spark_applications/`.
