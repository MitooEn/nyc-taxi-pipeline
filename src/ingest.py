import argparse
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date
from pyspark.sql.types import (
    StructType, StructField, TimestampType, LongType,
    DoubleType, IntegerType, StringType,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Define schema explicitly to make sure changes in parquet metadata got catch early
RAW_TRIP_SCHEMA = StructType([
    StructField("VendorID", IntegerType(), True),
    StructField("tpep_pickup_datetime", TimestampType(), True),
    StructField("tpep_dropoff_datetime", TimestampType(), True),
    StructField("passenger_count", LongType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("RatecodeID", LongType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("PULocationID", IntegerType(), True),
    StructField("DOLocationID", IntegerType(), True),
    StructField("payment_type", LongType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True),
    StructField("Airport_fee", DoubleType(), True),
    StructField("cbd_congestion_fee", DoubleType(), True),
])


def get_spark_session(app_name="ingest", master="spark://spark-master:7077"):
    spark = SparkSession.builder.appName(app_name).master(master).getOrCreate()
    # "dynamic" overwrites only if the partition match the data currently sent
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    return spark


def raw_path_for_month(year_month, base_path="/data/raw"):
    return f"{base_path}/yellow_tripdata_{year_month}.parquet"


def read_raw(spark, path, schema=RAW_TRIP_SCHEMA):
    return spark.read.schema(schema).parquet(path)


def add_pickup_date(df):
    return df.withColumn("pickup_date", to_date("tpep_pickup_datetime"))


def check_non_empty(row_count, name="Raw ingestion"):
    # Empty data guardrail
    if row_count == 0:
        logger.error(f"{name} produced zero rows")
        raise ValueError(
            f"{name} produced zero rows, check source's path file content"
        )
    logger.info(f"{name} passed non-empty check ({row_count} rows)")
    return row_count


def write_bronze(df, path="/data/bronze/batch/taxi_trips"):
    df.write.mode("overwrite").partitionBy("pickup_date").parquet(path)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Ingest one month of raw taxi data into bronze"
    )
    parser.add_argument(
        "--year-month",
        required=True,
        help="Target month to process, formatted YYYY-MM",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    year_month = args.year_month

    spark = get_spark_session()

    source_path = raw_path_for_month(year_month)
    logger.info(f"Reading raw data for {year_month} from {source_path}")
    df = read_raw(spark, source_path)

    df = add_pickup_date(df)

    check_non_empty(df.count(), name=f"Raw ingestion for {year_month}")

    logger.info("Writing to bronze layer:")
    write_bronze(df)

    logger.info(f"Ingestion complete for {year_month}")


if __name__ == "__main__":
    main()
