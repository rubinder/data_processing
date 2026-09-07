"""Mode-specific storage adapters for reading/writing data."""

import json
import os
from abc import ABC, abstractmethod
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from spark_applications.utils.mode import Mode


def _compact(df: DataFrame, partition_cols: list[str]) -> DataFrame:
    """Repartition by the output partition columns before writing.

    Without this, every Spark task may write a fragment into every output
    partition directory (tasks x partitions = small-file explosion, which is
    especially bad at hour grain). Repartitioning by the partition keys
    colocates each key's rows in a single task so each directory gets one
    file per write.
    """
    if not partition_cols:
        return df
    return df.repartition(*[df[col] for col in partition_cols])


def _read_csv_at(
    spark: SparkSession,
    full_path: str,
    schema: StructType | None,
    header: bool,
    corrupt_column: str | None,
) -> DataFrame:
    """Read a (optionally gzip) CSV at ``full_path``.

    Shared by every adapter so the schema/corrupt-record handling lives in
    one place. Spark reads ``.gz`` directly and distributed.
    """
    reader = spark.read.option("header", header)
    if schema is None:
        return reader.option("inferSchema", True).csv(full_path)
    reader = reader.schema(schema)
    if corrupt_column is not None:
        reader = (
            reader
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", corrupt_column)
        )
    return reader.csv(full_path)


def _parquet_partition_overwrite(
    df: DataFrame, full_path: str, partition_cols: list[str]
) -> None:
    """Write parquet with idempotent, partition-scoped overwrite.

    ``mode("overwrite")`` with a static partition spec deletes the *entire*
    table before writing, so reprocessing a single hour would destroy all
    other partitions. ``partitionOverwriteMode=dynamic`` scopes the overwrite
    to only the partitions present in ``df``, making a re-run of one
    (page_type, date, hour) idempotent and non-destructive to the rest.
    """
    df.sparkSession.conf.set(
        "spark.sql.sources.partitionOverwriteMode", "dynamic"
    )
    (
        _compact(df, partition_cols)
        .write
        .mode("overwrite")
        .partitionBy(*partition_cols)
        .parquet(full_path)
    )


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

    # -- raw-file primitives used by utils/landing.py -----------------------
    # These are deliberately tiny: the transactional behaviour (stage, write
    # table, promote, publish manifest, abort on failure) lives in one place,
    # ``landing.landed_raw_file``, and each adapter only has to know how to
    # test/move/delete an object and read/write a small JSON document.

    @abstractmethod
    def raw_file_exists(self, path: str) -> bool:
        """Return True if a raw object exists at ``path``."""

    @abstractmethod
    def move_raw_file(self, src: str, dst: str) -> None:
        """Move a raw object, replacing anything already at ``dst``."""

    @abstractmethod
    def delete_raw_file(self, path: str) -> None:
        """Delete a raw object; a missing object is not an error."""

    @abstractmethod
    def write_raw_manifest(self, manifest: dict, path: str) -> None:
        """Write a small JSON document into the raw area."""

    @abstractmethod
    def read_raw_manifest(self, path: str) -> dict | None:
        """Read a JSON document from the raw area; None when absent."""

    @abstractmethod
    def read_csv(
        self,
        spark: SparkSession,
        path: str,
        schema: StructType | None = None,
        header: bool = True,
        corrupt_column: str | None = None,
    ) -> DataFrame:
        """Read a (optionally gzip) CSV file into a DataFrame.

        When ``schema`` is provided no type inference is performed: the read
        is a single pass and the column contract is enforced. Spark reads
        ``.gz`` inputs directly and in a distributed manner, so callers must
        not decompress on the driver.

        When ``corrupt_column`` is set the read uses PERMISSIVE mode and
        routes unparseable rows into that column (which must be part of
        ``schema``) instead of failing or dropping them — enabling a
        quarantine path for schema-drifting input.
        """

    @abstractmethod
    def write_quarantine(self, df: DataFrame, path: str) -> None:
        """Append rows that failed the contract to a quarantine location.

        Appended (not overwritten) so a bad batch is preserved for inspection
        rather than discarded.
        """

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

    def _raw(self, path: str) -> str:
        return os.path.join(self.base_dir, "raw", path)

    def save_raw_file(self, content: bytes, path: str) -> None:
        full_path = self._raw(path)
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        with open(full_path, "wb") as f:
            f.write(content)

    def raw_file_exists(self, path: str) -> bool:
        return os.path.isfile(self._raw(path))

    def move_raw_file(self, src: str, dst: str) -> None:
        dst_full = self._raw(dst)
        os.makedirs(os.path.dirname(dst_full), exist_ok=True)
        # os.replace is atomic on the same filesystem: readers see either the
        # old raw file or the new one, never a partial write.
        os.replace(self._raw(src), dst_full)
        self._prune_empty_parent(self._raw(src))

    def delete_raw_file(self, path: str) -> None:
        full_path = self._raw(path)
        try:
            os.remove(full_path)
        except FileNotFoundError:
            return
        self._prune_empty_parent(full_path)

    def _prune_empty_parent(self, full_path: str) -> None:
        """Drop the immediate parent directory if we emptied it.

        Keeps ``raw/<table>/_staging/`` from accumulating one empty directory
        per job id.
        """
        parent = os.path.dirname(full_path)
        try:
            os.rmdir(parent)
        except OSError:
            pass

    def write_raw_manifest(self, manifest: dict, path: str) -> None:
        full_path = self._raw(path)
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        tmp_path = f"{full_path}.tmp"
        with open(tmp_path, "w") as f:
            json.dump(manifest, f, sort_keys=True, indent=2)
        os.replace(tmp_path, full_path)

    def read_raw_manifest(self, path: str) -> dict | None:
        try:
            with open(self._raw(path)) as f:
                return json.load(f)
        except FileNotFoundError:
            return None

    def read_csv(
        self,
        spark: SparkSession,
        path: str,
        schema: StructType | None = None,
        header: bool = True,
        corrupt_column: str | None = None,
    ) -> DataFrame:
        full_path = self._raw(path)
        return _read_csv_at(spark, full_path, schema, header, corrupt_column)

    def write_quarantine(self, df: DataFrame, path: str) -> None:
        full_path = os.path.join(self.base_dir, "quarantine", path)
        df.write.mode("append").parquet(full_path)

    def write_partitioned(
        self,
        df: DataFrame,
        path: str,
        partition_cols: list[str],
    ) -> None:
        full_path = os.path.join(self.base_dir, "processed", path)
        _parquet_partition_overwrite(df, full_path, partition_cols)

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
        _parquet_partition_overwrite(df, full_path, partition_cols)


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

    def _s3(self):
        import boto3

        return boto3.client("s3")

    def save_raw_file(self, content: bytes, path: str) -> None:
        self._s3().put_object(
            Bucket=self.bucket,
            Key=f"raw/{path}",
            Body=content,
        )

    def raw_file_exists(self, path: str) -> bool:
        from botocore.exceptions import ClientError

        try:
            self._s3().head_object(Bucket=self.bucket, Key=f"raw/{path}")
        except ClientError as exc:
            code = exc.response["Error"]["Code"]
            if code in ("404", "NoSuchKey", "NotFound"):
                return False
            raise
        return True

    def move_raw_file(self, src: str, dst: str) -> None:
        # S3 has no rename; copy then delete. A PUT of a single object is
        # atomic, so readers of ``dst`` see the old or the new object, never
        # a partial one. Objects over 5GB need multipart copy; raw hourly
        # files here are far smaller.
        s3 = self._s3()
        s3.copy_object(
            Bucket=self.bucket,
            Key=f"raw/{dst}",
            CopySource={"Bucket": self.bucket, "Key": f"raw/{src}"},
        )
        s3.delete_object(Bucket=self.bucket, Key=f"raw/{src}")

    def delete_raw_file(self, path: str) -> None:
        # delete_object on a missing key is a successful no-op in S3.
        self._s3().delete_object(Bucket=self.bucket, Key=f"raw/{path}")

    def write_raw_manifest(self, manifest: dict, path: str) -> None:
        self._s3().put_object(
            Bucket=self.bucket,
            Key=f"raw/{path}",
            Body=json.dumps(manifest, sort_keys=True, indent=2).encode(),
            ContentType="application/json",
        )

    def read_raw_manifest(self, path: str) -> dict | None:
        from botocore.exceptions import ClientError

        try:
            obj = self._s3().get_object(Bucket=self.bucket, Key=f"raw/{path}")
        except ClientError as exc:
            code = exc.response["Error"]["Code"]
            if code in ("404", "NoSuchKey", "NotFound"):
                return None
            raise
        return json.loads(obj["Body"].read())

    def read_csv(
        self,
        spark: SparkSession,
        path: str,
        schema: StructType | None = None,
        header: bool = True,
        corrupt_column: str | None = None,
    ) -> DataFrame:
        s3_path = f"s3a://{self.bucket}/raw/{path}"
        return _read_csv_at(spark, s3_path, schema, header, corrupt_column)

    def write_quarantine(self, df: DataFrame, path: str) -> None:
        s3_path = f"s3a://{self.bucket}/quarantine/{path}"
        df.write.mode("append").parquet(s3_path)

    def write_partitioned(
        self,
        df: DataFrame,
        path: str,
        partition_cols: list[str],
    ) -> None:
        s3_path = f"s3a://{self.bucket}/processed/{path}"
        _parquet_partition_overwrite(df, s3_path, partition_cols)

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
        _parquet_partition_overwrite(df, s3_path, partition_cols)


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

    # DBFS is mounted as a local filesystem at /dbfs, so the raw-file
    # primitives are the local ones against that root.
    def _raw(self, path: str) -> str:
        return f"/dbfs/raw/{path}"

    def save_raw_file(self, content: bytes, path: str) -> None:
        dbfs_path = self._raw(path)
        os.makedirs(os.path.dirname(dbfs_path), exist_ok=True)
        with open(dbfs_path, "wb") as f:
            f.write(content)

    def raw_file_exists(self, path: str) -> bool:
        return os.path.isfile(self._raw(path))

    def move_raw_file(self, src: str, dst: str) -> None:
        dst_full = self._raw(dst)
        os.makedirs(os.path.dirname(dst_full), exist_ok=True)
        os.replace(self._raw(src), dst_full)

    def delete_raw_file(self, path: str) -> None:
        try:
            os.remove(self._raw(path))
        except FileNotFoundError:
            pass

    def write_raw_manifest(self, manifest: dict, path: str) -> None:
        full_path = self._raw(path)
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        with open(full_path, "w") as f:
            json.dump(manifest, f, sort_keys=True, indent=2)

    def read_raw_manifest(self, path: str) -> dict | None:
        try:
            with open(self._raw(path)) as f:
                return json.load(f)
        except FileNotFoundError:
            return None

    def read_csv(
        self,
        spark: SparkSession,
        path: str,
        schema: StructType | None = None,
        header: bool = True,
        corrupt_column: str | None = None,
    ) -> DataFrame:
        dbfs_path = f"dbfs:/raw/{path}"
        return _read_csv_at(spark, dbfs_path, schema, header, corrupt_column)

    def _write_delta(
        self, df: DataFrame, table_name: str, partition_cols: list[str]
    ) -> None:
        # Delta scopes "overwrite" to the partitions present in df when
        # partitionOverwriteMode=dynamic, making reprocessing idempotent.
        (
            _compact(df, partition_cols)
            .write
            .format("delta")
            .mode("overwrite")
            .option("partitionOverwriteMode", "dynamic")
            .partitionBy(*partition_cols)
            .saveAsTable(table_name)
        )

    def write_quarantine(self, df: DataFrame, path: str) -> None:
        table_name = f"{self.catalog}.{self.schema}.quarantine_{path}"
        (
            df.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(table_name)
        )

    def write_partitioned(
        self,
        df: DataFrame,
        path: str,
        partition_cols: list[str],
    ) -> None:
        table_name = f"{self.catalog}.{self.schema}.{path}"
        self._write_delta(df, table_name, partition_cols)

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
        self._write_delta(df, table_name, partition_cols)


def get_storage_adapter(mode: Mode, **kwargs) -> StorageAdapter:
    """Factory function to get the appropriate storage adapter."""
    if mode == Mode.LOCAL:
        # PIPELINE_DATA_DIR lets the Airflow container and the Spark jobs
        # share one data root (status files, raw manifests) — see
        # airflow_deployment/dags/impression_quality_checks.py.
        return LocalStorageAdapter(
            base_dir=kwargs.get(
                "base_dir", os.getenv("PIPELINE_DATA_DIR", "data")
            )
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
