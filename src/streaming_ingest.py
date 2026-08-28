"""Simulate streaming into writing to bronze. It parses raw trip events off the
topic and appends them to /data/bronze/streaming/taxi_trips.

Separate batch's bronze and streaming's bronze. Spark streaming write create
a _spark_metadata (column name, data type, etc and log of what file it created). 
This cause any file in the same directory that isn't written by spark (not recorded in log) 
to be skipped at the next spark read (for example batch bronze if they share the same directory).
"""
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_date, when, isnan, lit
from pyspark.sql.types import (
    StructType, StructField, LongType,
    DoubleType, IntegerType, StringType,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

"""from_json runs in PERMISSIVE mode by default. Unparsable message returns a struct of null rather 
than a null struct, which is indistinguishable from a well-formed message carrying no values. 
columnNameOfCorruptRecord make Spark write the original text into it on parse failure. This also help
in differentiating struct of nulls and genuine empty row.
"""
CORRUPT_RECORD_COLUMN = "_corrupt_record"

"""Mirrors RAW_TRIP_SCHEMA in ingest.py, except:
    1. Timestamps -> StringType. The producer serializes them to json strings,
    and from_json will not turn it back to timestamp.

    2. passenger_count and RatecodeID changed from LongType to DoubleType. 
    The producer reads the source Parquet with pandas, whose default 
    int64 dtype cannot hold nulls (or in this case NaN). When NaN is detected, 
    it will transform the entire column to float64 instead (can hold NaN) causing
    all non NaN rows to become float.

    When float/double is written against LongType, from_json (through spark) treat 
    the type mismatch as a parse failure. This send the entire row to _corrupt_record
    even though the other fields parse correctly, which then routes everything to the DLQ.
"""
TRIP_MESSAGE_SCHEMA = StructType([
    StructField("VendorID", IntegerType(), True),
    StructField("tpep_pickup_datetime", StringType(), True),
    StructField("tpep_dropoff_datetime", StringType(), True),
    StructField("passenger_count", DoubleType(), True),      # float64 via pandas
    StructField("trip_distance", DoubleType(), True),
    StructField("RatecodeID", DoubleType(), True),           # float64 via pandas
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
    StructField(CORRUPT_RECORD_COLUMN, StringType(), True),
])


def get_spark_session(app_name="streaming_ingest", master="spark://spark-master:7077"):
    spark = SparkSession.builder.appName(app_name).master(master).getOrCreate()
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    return spark


def read_kafka_stream(spark, kafka_bootstrap="kafka:9092", topic="taxi-trips-raw"):
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap) \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .load()


def parse_messages(raw_stream_df):
    # return dataframe with 2 column: raw_value and trip
    return raw_stream_df.select(
        # Extract raw bytes and cast to string
        col("value").cast("string").alias("raw_value"),
        # Extract content and parse against schema, if all field is null sent casted value to CORRUPT_RECORD_COLUMN
        from_json(
            col("value").cast("string"),
            TRIP_MESSAGE_SCHEMA,
            {"columnNameOfCorruptRecord": CORRUPT_RECORD_COLUMN},
        ).alias("trip"),
    )


def split_valid_invalid(parsed_df):
    corrupt = col(f"trip.{CORRUPT_RECORD_COLUMN}")

    valid = parsed_df.filter(corrupt.isNull()) \
        .select("trip.*") \
        .drop(CORRUPT_RECORD_COLUMN)

    invalid = parsed_df.filter(corrupt.isNotNull()).select("raw_value")

    return valid, invalid


def prepare_for_bronze(valid_df):
    # Change back the datatype
    return valid_df \
        .withColumn("tpep_pickup_datetime", col("tpep_pickup_datetime").cast("timestamp")) \
        .withColumn("tpep_dropoff_datetime", col("tpep_dropoff_datetime").cast("timestamp")) \
        .withColumn("passenger_count",
            when(isnan(col("passenger_count")), lit(None).cast("long"))
            .otherwise(col("passenger_count").cast("long")))\
        .withColumn("RatecodeID",
            when(isnan(col("RatecodeID")), lit(None).cast("long"))
            .otherwise(col("RatecodeID").cast("long")))\
        .withColumn("pickup_date", to_date(col("tpep_pickup_datetime")))


def build_dlq_payload(invalid_df):
    # Kafka sink need "value" column
    return invalid_df.select(col("raw_value").alias("value"))


def main():
    spark = get_spark_session()

    raw_stream = read_kafka_stream(spark)
    parsed = parse_messages(raw_stream)
    valid, invalid = split_valid_invalid(parsed)

    logger.info("Starting bronze sink:")
    # Different checkpoint to prevent conflictiing metadata
    bronze_query = prepare_for_bronze(valid).writeStream \
        .outputMode("append") \
        .format("parquet") \
        .partitionBy("pickup_date") \
        .trigger(processingTime="10 seconds") \
        .option("checkpointLocation", "/data/checkpoints/streaming_ingest_bronze") \
        .option("path", "/data/bronze/streaming/taxi_trips") \
        .start()

    logger.info("Starting dead-letter sink:")
    dlq_query = build_dlq_payload(invalid).writeStream \
        .outputMode("append") \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("topic", "taxi-trips-dlq") \
        .trigger(processingTime="10 seconds") \
        .option("checkpointLocation", "/data/checkpoints/streaming_ingest_dlq") \
        .start()
    
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()

# check dlq content:
# docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --topic taxi-trips-dlq --bootstrap-server localhost:9092 --from-beginning --timeout-ms 5000