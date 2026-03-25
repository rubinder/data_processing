FROM python:3.10-slim

RUN pip install --no-cache-dir \
    dbt-core==1.9.4 \
    dbt-postgres==1.9.4

WORKDIR /dbt

COPY dbt_project/ /dbt/

ENTRYPOINT ["dbt"]
CMD ["run"]
