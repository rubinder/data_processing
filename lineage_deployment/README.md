# Lineage Deployment (Marquez)

Runs [Marquez](https://marquezproject.ai), the OpenLineage reference backend,
so the pipelines in this repo can report what they read and wrote. Three
containers: `marquez-api` (the `POST /api/v1/lineage` endpoint on port 5000),
`marquez-web` (graph UI on port 3000) and a PostgreSQL.

```bash
./deploy.sh up            # shared data-processing-network (Airflow, Spark, dbt reach http://marquez-api:5000)
./deploy.sh up local      # standalone network
./deploy.sh smoke         # posts a synthetic START/COMPLETE run; proves the endpoint
./deploy.sh namespaces    # what has reported in
./deploy.sh jobs          # jobs in the data_processing namespace
./deploy.sh down
```

Nothing emits until it is told to. Every emitter in the repo keys off one
variable, `OPENLINEAGE_URL` (plus `OPENLINEAGE_NAMESPACE`, default
`data_processing`); unset, they run exactly as before. How each layer emits
and what the resulting graph looks like is in `LINEAGE.md`.


## Architecture

Marquez as the OpenLineage backend, and every emitter in the repository that can point at it.

```mermaid
flowchart LR
    subgraph module["lineage_deployment"]
        api["marquez-api<br/>OpenLineage API, localhost:5000"]
        web["marquez-web<br/>UI localhost:3000"]
        db[("marquez-db<br/>postgres:15")]
        replay["replay_cloudwatch.py<br/>Glue and EMR events from CloudWatch"]
        deploy["deploy.sh"]
    end
    subgraph emitters["Emitters, all gated on OPENLINEAGE_URL"]
        spark["spark_applications<br/>Spark listener in utils/session.py"]
        airflow["airflow_deployment<br/>OpenLineage provider"]
        dbt["dbt_deployment<br/>dbt-ol wrapper"]
        glue["aws_deployment<br/>Glue job, EMR jar, lineage_sink Lambda"]
        athena["aws_deployment<br/>VerifyInAthena manual emission"]
    end
    deploy --> api
    api --> db
    web --> api
    spark -.-> api
    airflow -.-> api
    dbt -.-> api
    athena -.-> api
    glue -.->|"CloudWatch"| replay
    replay --> api
```

- `LINEAGE.md` describes each emitter and what was verified live; the Glue and EMR path was replayed into local Marquez from the stack's capture endpoint.
