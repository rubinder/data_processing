"""Mode-specific storage adapters for reading/writing data."""

import json
import os
from abc import ABC, abstractmethod
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession

from spark_applications.utils.mode import Mode


class StorageAdapter(ABC):
    """Abstract base class for mode-specific storage operations."""

    @abstractmethod
    def update_status(
        self, spark: SparkSession, job_id: str, status: str
    ) -> None:
        """Update processing status for a job run."""

    @abstractmethod
    def save_raw_file(self, content: bytes, path: str) -> None:
        """Save raw file bytes to storage."""

    @abstractmethod
    def read_csv(
        self, spark: SparkSession, path: str, header: bool = True
    ) -> DataFrame:
        """Read a CSV file into a DataFrame."""

    @abstractmethod
    def write_partitioned(
        self,
        df: DataFrame,
        path: str,
        partition_cols: list[str],
    ) -> None:
        """Write DataFrame partitioned by given columns."""

    @abstractmethod
    def read_partitioned(
        self, spark: SparkSession, path: str
    ) -> DataFrame:
        """Read partitioned data from storage."""

    @abstractmethod
    def write_output(
        self,
        df: DataFrame,
        path: str,
        partition_cols: list[str],
    ) -> None:
        """Write aggregated output data."""


class LocalStorageAdapter(StorageAdapter):
    """Storage adapter for local filesystem."""

    def __init__(self, base_dir: str = "data"):
        self.base_dir = base_dir

    def update_status(
        self, spark: SparkSession, job_id: str, status: str
    ) -> None:
        status_dir = os.path.join(self.base_dir, "status")
        os.makedirs(status_dir, exist_ok=True)
        status_file = os.path.join(status_dir, f"{job_id}.json")
        record = {
            "job_id": job_id,
            "status": status,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        with open(status_file, "w") as f:
            json.dump(record, f)

    def save_raw_file(self, content: bytes, path: str) -> None:
        full_path = os.path.join(self.base_dir, "raw", path)
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        with open(full_path, "wb") as f:
            f.write(content)

    def read_csv(
        self, spark: SparkSession, path: str, header: bool = True
    ) -> DataFrame:
        full_path = os.path.join(self.base_dir, "raw", path)
        return spark.read.csv(full_path, header=header, inferSchema=True)

    def write_partitioned(
        self,
        df: DataFrame,
        path: str,
        partition_cols: list[str],
    ) -> None:
        full_path = os.path.join(self.base_dir, "processed", path)
        (
            df.write
            .mode("overwrite")
            .partitionBy(*partition_cols)
            .parquet(full_path)
        )

    def read_partitioned(
        self, spark: SparkSession, path: str
    ) -> DataFrame:
        full_path = os.path.join(self.base_dir, "processed", path)
        return spark.read.parquet(full_path)

    def write_output(
        self,
        df: DataFrame,
        path: str,
        partition_cols: list[str],
    ) -> None:
        full_path = os.path.join(self.base_dir, "output", path)
        (
            df.write
            .mode("overwrite")
            .partitionBy(*partition_cols)
            .parquet(full_path)
        )


class AwsStorageAdapter(StorageAdapter):
    """Storage adapter for AWS (S3 + DynamoDB)."""

    def __init__(self, bucket: str, table_name: str):
        self.bucket = bucket
        self.table_name = table_name

    def _get_dynamodb_table(self):
        import boto3

        dynamodb = boto3.resource("dynamodb")
        return dynamodb.Table(self.table_name)

    def update_status(
        self, spark: SparkSession, job_id: str, status: str
    ) -> None:
        table = self._get_dynamodb_table()
        table.put_item(
            Item={
                "job_id": job_id,
                "status": status,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
        )

    def save_raw_file(self, content: bytes, path: str) -> None:
        import boto3

        s3 = boto3.client("s3")
        s3.put_object(
            Bucket=self.bucket,
            Key=f"raw/{path}",
            Body=content,
        )

    def read_csv(
        self, spark: SparkSession, path: str, header: bool = True
    ) -> DataFrame:
        s3_path = f"s3a://{self.bucket}/raw/{path}"
        return spark.read.csv(s3_path, header=header, inferSchema=True)

    def write_partitioned(
        self,
        df: DataFrame,
        path: str,
        partition_cols: list[str],
    ) -> None:
        s3_path = f"s3a://{self.bucket}/processed/{path}"
        (
            df.write
            .mode("overwrite")
            .partitionBy(*partition_cols)
            .parquet(s3_path)
        )

    def read_partitioned(
        self, spark: SparkSession, path: str
    ) -> DataFrame:
        s3_path = f"s3a://{self.bucket}/processed/{path}"
        return spark.read.parquet(s3_path)

    def write_output(
        self,
        df: DataFrame,
        path: str,
        partition_cols: list[str],
    ) -> None:
        s3_path = f"s3a://{self.bucket}/output/{path}"
        (
            df.write
            .mode("overwrite")
            .partitionBy(*partition_cols)
            .parquet(s3_path)
        )


class DatabricksStorageAdapter(StorageAdapter):
    """Storage adapter for Databricks (Delta tables + DBFS)."""

    def __init__(self, catalog: str = "default", schema: str = "default"):
        self.catalog = catalog
        self.schema = schema

    def update_status(
        self, spark: SparkSession, job_id: str, status: str
    ) -> None:
        from pyspark.sql import Row

        status_table = f"{self.catalog}.{self.schema}.job_status"
        row = Row(
            job_id=job_id,
            status=status,
            updated_at=datetime.now(timezone.utc).isoformat(),
        )
        df = spark.createDataFrame([row])
        (
            df.write
            .format("delta")
            .mode("append")
            .saveAsTable(status_table)
        )

    def save_raw_file(self, content: bytes, path: str) -> None:
        dbfs_path = f"/dbfs/raw/{path}"
        os.makedirs(os.path.dirname(dbfs_path), exist_ok=True)
        with open(dbfs_path, "wb") as f:
            f.write(content)

    def read_csv(
        self, spark: SparkSession, path: str, header: bool = True
    ) -> DataFrame:
        dbfs_path = f"dbfs:/raw/{path}"
        return spark.read.csv(dbfs_path, header=header, inferSchema=True)

    def write_partitioned(
        self,
        df: DataFrame,
        path: str,
        partition_cols: list[str],
    ) -> None:
        table_name = f"{self.catalog}.{self.schema}.{path}"
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .partitionBy(*partition_cols)
            .saveAsTable(table_name)
        )

    def read_partitioned(
        self, spark: SparkSession, path: str
    ) -> DataFrame:
        table_name = f"{self.catalog}.{self.schema}.{path}"
        return spark.read.table(table_name)

    def write_output(
        self,
        df: DataFrame,
        path: str,
        partition_cols: list[str],
    ) -> None:
        table_name = f"{self.catalog}.{self.schema}.{path}"
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .partitionBy(*partition_cols)
            .saveAsTable(table_name)
        )


def get_storage_adapter(mode: Mode, **kwargs) -> StorageAdapter:
    """Factory function to get the appropriate storage adapter."""
    if mode == Mode.LOCAL:
        return LocalStorageAdapter(
            base_dir=kwargs.get("base_dir", "data")
        )
    elif mode == Mode.AWS:
        return AwsStorageAdapter(
            bucket=kwargs.get("bucket", os.getenv("S3_BUCKET", "")),
            table_name=kwargs.get(
                "table_name",
                os.getenv("DYNAMODB_TABLE", ""),
            ),
        )
    elif mode == Mode.DATABRICKS:
        return DatabricksStorageAdapter(
            catalog=kwargs.get("catalog", "default"),
            schema=kwargs.get("schema", "default"),
        )
    else:
        raise ValueError(f"Unknown mode: {mode}")
