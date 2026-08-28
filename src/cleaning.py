import argparse
import logging
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def get_spark_session(app_name="cleaning", master="spark://spark-master:7077"):
    spark = SparkSession.builder.appName(app_name).master(master).getOrCreate()
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    return spark


def read_bronze(spark, path="/data/bronze/batch/taxi_trips"):
    return spark.read.parquet(path)


def filter_to_month(df, year_month):
    """Filter spillover from previous/next month
    Compares pickup_date against a date range rather than applying date_format
    to it. This is to ensure partition pruning is triggered. With date_format, spark
    can't map it back to the directories and end up scanning everything
    """
    year, month = map(int, year_month.split("-"))
    start = date(year, month, 1)
    end = date(year + month // 12, month % 12 + 1, 1)
    return df.filter((col("pickup_date") >= start) & (col("pickup_date") < end))


def drop_duplicates(df):
    return df.dropDuplicates()


def fill_optional_fees(df):
    # Fill nulls in fee/surcharge columns and passenger_count with documented or imputed defaults.
    return df.fillna({
        # ~29% null rate; imputed rather than dropped (see README)
        "passenger_count": 1,
        "extra": 0,
        "mta_tax": 0.5,  # verified 2026 rate
        "tip_amount": 0,
        "tolls_amount": 0,
        "improvement_surcharge": 1,  # verified 2026 rate
        "congestion_surcharge": 0,
        "Airport_fee": 0,
        "cbd_congestion_fee": 0,
    })


def drop_incomplete_core_fields(df):
    return df.dropna(subset=["tpep_pickup_datetime", "fare_amount", "total_amount"])


def filter_invalid_trips(df):
    return df.filter(
        (col("tpep_dropoff_datetime") > col("tpep_pickup_datetime")) &
        (col("passenger_count") > 0) &
        (col("trip_distance") > 0) & (col("trip_distance") <= 100) &
        (col("PULocationID").between(1, 263)) &
        (col("DOLocationID").between(1, 263)) &
        (col("fare_amount") >= 0) &
        (col("tip_amount") >= 0) &
        (col("total_amount") >= 0)
    )


def check_drop_rate(raw_count, clean_count, threshold_pct=10):
    # Prevent ZeroDivisionError
    if raw_count == 0:
        logger.error("Bronze layer contained zero rows for this month")
        raise ValueError(
            "No bronze rows found for the target month. Check if ingest.py run"
        )

    dropped = raw_count - clean_count
    pct_dropped = (dropped / raw_count) * 100

    logger.info(f"Silver row count: {clean_count}")
    logger.info(f"Dropped {dropped} rows ({pct_dropped:.2f}%)")

    if pct_dropped > threshold_pct:
        # Data quality (how many is dropped) guardrail
        logger.error(
            f"Drop rate {pct_dropped:.2f}% exceeds {threshold_pct}% threshold"
        )
        raise ValueError(
            f"Data quality check failed: dropped {pct_dropped:.2f}% of rows, "
            f"exceeds {threshold_pct}% threshold"
        )

    return pct_dropped


def write_silver(df, path="/data/silver/taxi_trips"):
    df.write.mode("overwrite").partitionBy("pickup_date").parquet(path)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Clean one month of bronze data into silver"
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

    logger.info("Reading bronze data:")
    df = read_bronze(spark)

    df_clean = filter_to_month(df, year_month)
    month_count = df_clean.count()
    logger.info(f"Bronze rows for {year_month}: {month_count}")

    df_clean = drop_duplicates(df_clean)
    df_clean = fill_optional_fees(df_clean)
    df_clean = drop_incomplete_core_fields(df_clean)
    df_clean = filter_invalid_trips(df_clean)

    clean_count = df_clean.count()
    check_drop_rate(month_count, clean_count)

    logger.info("Writing to silver layer:")
    write_silver(df_clean)

    logger.info(f"Cleaning complete for {year_month}")


if __name__ == "__main__":
    main()
