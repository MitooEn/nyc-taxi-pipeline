# Manual inspection of bronze layer
import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, when, min as spark_min, max as spark_max,
)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Inspect a bronze layer."
    )
    parser.add_argument(
        "--path",
        default="/data/bronze/streaming/taxi_trips",
        help="Bronze path to read (default: streaming path)",
    )
    parser.add_argument(
        "--show-partitions",
        action="store_true",
        help="List every partition date",
    )
    return parser.parse_args()


def summarise_partitions(df):
    return df.select("pickup_date").distinct().select(
        count("*").alias("distinct_dates"),
        spark_min("pickup_date").alias("earliest"),
        spark_max("pickup_date").alias("latest"),
    )


def summarise_nulls(df):
    """Spark cannot use a null as a partition value, so null's existance imply the existance
    of __HIVE_DEFAULT_PARTITION__ directory. The directory's presence means there are some
    message that should have gone to kafka's dlq query instead
    """
    return df.select(
        count("*").alias("total_rows"),
        count(when(col("pickup_date").isNull(), 1)).alias("null_pickup_date"),
        count(when(col("passenger_count").isNull(), 1)).alias("null_passengers"),
        count(when(col("RatecodeID").isNull(), 1)).alias("null_ratecode"),
    )


def main():
    args = parse_args()

    spark = SparkSession.builder \
        .appName("read_parquet_bronze") \
        .master("spark://spark-master:7077") \
        .getOrCreate()

    df = spark.read.parquet(args.path)

    summarise_partitions(df).show(truncate=False)
    summarise_nulls(df).show(truncate=False)

    if args.show_partitions:
        df.select("pickup_date").distinct() \
            .orderBy("pickup_date").show(100, truncate=False)

    df.select(
        "tpep_pickup_datetime", "PULocationID", "DOLocationID",
        "passenger_count", "RatecodeID", "fare_amount", "pickup_date",
    ).orderBy("tpep_pickup_datetime").show(10, truncate=False)


if __name__ == "__main__":
    main()

# run: docker exec -it spark-master bash
# run: /opt/spark/bin/spark-submit /tools/read_parquet_bronze.py
# run: /opt/spark/bin/spark-submit /tools/read_parquet_bronze.py --path /data/bronze/batch/taxi_trips
# run: /opt/spark/bin/spark-submit /tools/read_parquet_bronze.py --show-partitions