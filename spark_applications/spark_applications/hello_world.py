"""Hello world PySpark job."""

from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder \
        .appName("HelloWorld") \
        .master("local[*]") \
        .getOrCreate()

    data = [
        ("hello", 1),
        ("world", 2),
        ("from", 3),
        ("spark", 4),
        ("applications", 5),
    ]
    df = spark.createDataFrame(data, ["word", "count"])

    print("Hello World from Spark Applications!")
    df.show()

    total = df.agg({"count": "sum"}).collect()[0][0]
    print(f"Total count: {total}")

    spark.stop()


if __name__ == "__main__":
    main()
