FROM python:3.10-slim

RUN pip install --no-cache-dir \
    dbt-core==1.9.1 \
    dbt-postgres==1.9.1 \
    # dbt-ol wrapper: emits OpenLineage events when OPENLINEAGE_URL is set
    # (see deploy.sh); inert otherwise.
    openlineage-dbt==1.53.0

WORKDIR /dbt

COPY dbt_project/ /dbt/

ENTRYPOINT ["dbt"]
CMD ["run"]
