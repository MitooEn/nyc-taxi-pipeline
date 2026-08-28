import argparse
import logging
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def get_spark_session(app_name="aggregate", master="spark://spark-master:7077"):
    spark = SparkSession.builder.appName(app_name).master(master).getOrCreate()
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    return spark


def read_silver(spark, year_month, path="/data/silver/taxi_trips"):
    year, month = map(int, year_month.split("-"))
    start = date(year, month, 1)
    end = date(year + month // 12, month % 12 + 1, 1)
    df = spark.read.parquet(path)
    return df.filter((F.col("pickup_date") >= start) & (F.col("pickup_date") < end))


def read_zone_lookup(spark, path="/data/reference/taxi_zone_lookup.csv"):
    return spark.read.option("header", "true").option("inferSchema", "true").csv(path)


def build_daily_summary(df):
    return df.groupBy("pickup_date").agg(
        F.count("*").alias("trip_count"),
        F.round(F.sum("total_amount"), 2).alias("total_revenue"),
        F.round(F.avg("total_amount"), 2).alias("avg_fare"),
    ).orderBy("pickup_date")


def build_hourly_summary(df):
    return df.withColumn("pickup_hour", F.hour("tpep_pickup_datetime")) \
        .groupBy("pickup_hour").agg(
            F.count("*").alias("trip_count"),
            F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
            F.round(F.avg("tip_amount"), 2).alias("avg_tip"),
        ).orderBy("pickup_hour")


def build_location_summary(df, zone_lookup):
    location_counts = df.groupBy("PULocationID").agg(
        F.count("*").alias("trip_count"),
        F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
        F.round(F.avg("tip_amount"), 2).alias("avg_tip"),
    )

    return location_counts.join(
        zone_lookup,
        location_counts.PULocationID == zone_lookup.LocationID,
        "left",
    ).select(
        "PULocationID",
        F.col("Zone").alias("pickup_zone_name"),
        F.col("Borough").alias("pickup_borough"),
        "trip_count", "avg_fare", "avg_tip",
    ).orderBy(F.col("trip_count").desc())


def build_borough_summary(df, zone_lookup):
    return df.join(
        zone_lookup,
        df.PULocationID == zone_lookup.LocationID,
        "left",
    ).groupBy("Borough").agg(
        F.count("*").alias("trip_count"),
        F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
        F.round(F.avg("total_amount"), 2).alias("avg_total"),
    ).orderBy(F.col("trip_count").desc())


def check_non_empty(df, name):
    # Empty data guardrail
    row_count = df.count()
    if row_count == 0:
        logger.error(f"{name} produced zero rows")
        raise ValueError(
            f"{name} produced zero rows, check upstream silver data"
        )
    logger.info(f"{name} passed non-empty check ({row_count} rows)")
    return row_count


def write_gold(df, path, year_month):
    df.withColumn("year_month", F.lit(year_month)) \
        .write.mode("overwrite").partitionBy("year_month").parquet(path)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Aggregate one month of silver data into gold."
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

    logger.info(f"Reading silver data for {year_month}")
    df = read_silver(spark, year_month).cache()
    zone_lookup = read_zone_lookup(spark)
    logger.info(f"Silver row count for {year_month}: {df.count()}")

    logger.info(f"Building daily summary of {year_month}:")
    daily_summary = build_daily_summary(df)
    check_non_empty(daily_summary, "Daily summary")
    write_gold(daily_summary, "/data/gold/daily_summary", year_month)
    logger.info(f"Finished writing daily summary of {year_month}")

    logger.info(f"Building hourly summary of {year_month}:")
    hourly_summary = build_hourly_summary(df)
    check_non_empty(hourly_summary, "Hourly summary")
    write_gold(hourly_summary, "/data/gold/hourly_summary", year_month)
    logger.info(f"Finished writing hourly summary of {year_month}")

    logger.info(f"Building pickup location summary of {year_month}:")
    pickup_location_summary = build_location_summary(df, zone_lookup)
    check_non_empty(pickup_location_summary, "Pickup location summary")
    write_gold(pickup_location_summary, "/data/gold/pickup_location_summary", year_month)
    logger.info(f"Finished writing pickup location summary of {year_month}")

    logger.info(f"Building borough summary of {year_month}:")
    borough_summary = build_borough_summary(df, zone_lookup)
    check_non_empty(borough_summary, "Borough summary")
    write_gold(borough_summary, "/data/gold/borough_summary", year_month)
    logger.info(f"Finished writing borough summary of {year_month}")

    df.unpersist()
    logger.info(f"Aggregation complete for {year_month}")


if __name__ == "__main__":
    main()

# NOTE: no .cache() and no manual spark.sql.shuffle.partitions override, see README
# "Performance investigation" section for the reasoning (AQE + cardinality findings).