"""Simulate spark streaming. 

Computes net flow per zone in short sliding windows: a pickup removes an
available car from a zone, a dropoff adds one back. Positive net flow means
more cars arrived than left, negative means demand is drawing them away.
"""
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, lit, to_json, struct, sum as spark_sum,
)
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Only the fields this job needs.
TRIP_MESSAGE_SCHEMA = StructType([
    StructField("tpep_pickup_datetime", StringType(), True),
    StructField("tpep_dropoff_datetime", StringType(), True),
    StructField("PULocationID", IntegerType(), True),
    StructField("DOLocationID", IntegerType(), True),
])


def get_spark_session(app_name="streaming_analytics", master="spark://spark-master:7077"):
    return SparkSession.builder.appName(app_name).master(master).getOrCreate()


def read_kafka_stream(spark, kafka_bootstrap="kafka:9092", topic="taxi-trips-raw"):
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap) \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .load()


def parse_messages(raw_stream_df):
    parsed = raw_stream_df.select(
        from_json(col("value").cast("string"), TRIP_MESSAGE_SCHEMA).alias("trip")
    ).select("trip.*")

    return parsed \
        .withColumn("pickup_time", col("tpep_pickup_datetime").cast("timestamp")) \
        .withColumn("dropoff_time", col("tpep_dropoff_datetime").cast("timestamp"))


def to_flow_events(parsed_df):
    # Split each trip into two supply events and union them.
    pickups = parsed_df.select(
        col("PULocationID").alias("zone_id"),
        col("pickup_time").alias("event_time"),
        lit(-1).alias("delta"),
    ).filter(col("event_time").isNotNull())

    dropoffs = parsed_df.select(
        col("DOLocationID").alias("zone_id"),
        col("dropoff_time").alias("event_time"),
        lit(1).alias("delta"),
    ).filter(col("event_time").isNotNull())

    return pickups.unionByName(dropoffs)


def build_net_flow(events_df, window_duration="15 minutes",
                   slide_duration="5 minutes", watermark_delay="45 minutes"):
    """Net flow per zone, in 15-minute windows updated every 5 minutes.

    Accept late message up to 45 minutes from latest event_time.
    Producer sample data and order them by tpep_pickup_datetime. Say, the earliest
    pickup may have a pretty late dropoff. Without sufficient watermark, the next
    earliest pickup can get dropped.

    window create a struct {start:, end:}
    """
    return events_df \
        .withWatermark("event_time", watermark_delay) \
        .groupBy(
            window(col("event_time"), window_duration, slide_duration),
            col("zone_id"),
        ).agg(
            spark_sum("delta").alias("taxi_net_flow")
        )


def build_kafka_payload(net_flow_df):
    """Serialise the aggregate for the Kafka sink.

    The sink requires a value column of string or binary. Keying by zone
    cause all updates for a zone to land on the same partition and stay in order.
    Could be useful to track a zone's state over time. But for now, there is only 1 partition
    so it doesn't do anything meaningful.
    """
    return net_flow_df.select(
        col("zone_id").cast("string").alias("key"),
        # Turn it into a single dict
        to_json(struct(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("zone_id"),
            col("taxi_net_flow"),
        )).alias("value"),
    )


def main():
    spark = get_spark_session()

    raw_stream = read_kafka_stream(spark)
    parsed = parse_messages(raw_stream)
    events = to_flow_events(parsed)
    net_flow = build_net_flow(events)

    logger.info("Starting Kafka sink:")
    kafka_query = build_kafka_payload(net_flow).writeStream \
        .outputMode("update") \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("topic", "zone-net-flow") \
        .trigger(processingTime="10 seconds") \
        .option("checkpointLocation", "/data/checkpoints/streaming_analytics_kafka") \
        .start()

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
