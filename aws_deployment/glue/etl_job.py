"""AWS Glue ETL job: reads raw CSV from S3, writes partitioned parquet."""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "source_bucket",
    "source_key",
    "target_bucket",
    "database_name",
    "table_name",
])

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

source_path = f"s3://{args['source_bucket']}/{args['source_key']}"
target_path = f"s3://{args['target_bucket']}/processed/"

df = spark.read.option("header", "true").option("inferSchema", "true").csv(
    source_path
)

df = df.withColumn("date", F.col("date").cast("string"))
df = df.withColumn("hour", F.col("hour").cast("string"))
df = df.withColumn("page_type", F.col("page_type").cast("string"))

df.write.mode("append").partitionBy(
    "page_type", "date", "hour"
).parquet(target_path)

print(
    f"Wrote {df.count()} rows to {target_path} "
    f"partitioned by page_type/date/hour"
)

job.commit()
