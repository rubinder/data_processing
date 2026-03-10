"""Hello world PySpark script for AWS EMR."""

import sys

from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder \
        .appName("HelloWorldEMR") \
        .getOrCreate()

    data = [("hello", 1), ("world", 2), ("from", 3), ("emr", 4), ("spark", 5)]
    df = spark.createDataFrame(data, ["word", "count"])

    print("Hello World from EMR Spark!")
    df.show()

    total = df.agg({"count": "sum"}).collect()[0][0]
    print(f"Total count: {total}")

    spark.stop()


if __name__ == "__main__":
    main()
