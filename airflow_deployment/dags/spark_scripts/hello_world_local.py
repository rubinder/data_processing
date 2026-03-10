"""Hello world PySpark script for local Spark cluster."""

from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder \
        .appName("HelloWorldLocal") \
        .getOrCreate()

    data = [("hello", 1), ("world", 2), ("from", 3), ("local", 4), ("spark", 5)]
    df = spark.createDataFrame(data, ["word", "count"])

    print("Hello World from Local Spark!")
    df.show()

    total = df.agg({"count": "sum"}).collect()[0][0]
    print(f"Total count: {total}")

    spark.stop()


if __name__ == "__main__":
    main()
