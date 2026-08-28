import argparse
import logging

from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# JDBC as a connector between spark and clickhouse
JDBC_URL = "jdbc:clickhouse://clickhouse:8123/default"
JDBC_DRIVER = "com.clickhouse.jdbc.ClickHouseDriver"
JDBC_USER = "user1"
JDBC_PASSWORD = "pass1"

# Gold tables to load.
TABLES = [
    "daily_summary",
    "hourly_summary",
    "pickup_location_summary",
    "borough_summary",
]


def get_spark_session(app_name="load_clickhouse", master="spark://spark-master:7077"):
    return SparkSession.builder.appName(app_name).master(master).getOrCreate()


def read_gold(spark, table, year_month, path_base="/data/gold"):
    df = spark.read.parquet(f"{path_base}/{table}")
    return df.filter(df.year_month == year_month)


def check_non_empty(row_count, table, year_month):
    # Empty data guardrail
    if row_count == 0:
        logger.error(f"gold/{table} produced zero rows for {year_month}")
        raise ValueError(
            f"No rows found in gold/{table} for {year_month}, check upstream gold data"
        )
    logger.info(f"{table}: {row_count} rows for {year_month}")
    return row_count


def drop_month(spark, table, year_month):
    # Ensure idempotency
    jvm = spark._jvm # Access to the JVM Spark runs on
    jvm.java.lang.Class.forName(JDBC_DRIVER) # Java's method to load JDBC driver class and ensures driver is registered with java.sql.DriverManager
    props = jvm.java.util.Properties() # Key-value store for connection parameters
    props.setProperty("user", JDBC_USER)
    props.setProperty("password", JDBC_PASSWORD)

    conn = jvm.java.sql.DriverManager.getConnection(JDBC_URL, props)
    try:
        stmt = conn.createStatement()
        sql = f"ALTER TABLE {table} DROP PARTITION '{year_month}'"
        logger.info(f"Executing: {sql}")
        stmt.execute(sql)
        stmt.close()
    finally:
        conn.close()


def write_table(df, table):
    df.write \
        .format("jdbc") \
        .option("url", JDBC_URL) \
        .option("driver", JDBC_DRIVER) \
        .option("dbtable", table) \
        .option("user", JDBC_USER) \
        .option("password", JDBC_PASSWORD) \
        .mode("append") \
        .save()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Load one month of gold summaries into ClickHouse"
    )
    parser.add_argument(
        "--year-month",
        required=True,
        help="Month to load, formatted YYYY-MM",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    year_month = args.year_month

    spark = get_spark_session()

    for table in TABLES:
        df = read_gold(spark, table, year_month)
        check_non_empty(df.count(), table, year_month)

        drop_month(spark, table, year_month)
        write_table(df, table)
        logger.info(f"{table}: loaded")

    logger.info(f"ClickHouse load complete for {year_month}")


if __name__ == "__main__":
    main()


