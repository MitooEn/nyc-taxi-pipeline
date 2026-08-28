# Read raw data to simulate streaming
import argparse
import json
import logging
import time

import pandas as pd
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def get_producer(bootstrap_servers="localhost:29092"):
    # Connects to EXTERNAL listener because this script runs on the host. Serialized into json
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        # Turn message into json text string then encode to bytes using utf-8 formatting
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
    )


def raw_path_for_month(year_month, base_path="./data/raw"):
    return f"{base_path}/yellow_tripdata_{year_month}.parquet"


def load_trips(path, year_month, day=None, start_hour=None, end_hour=None, n=1000):
    """Sample some raw data to load to kafka. day, start_hour, and end_hour
    is there to restrict stream to a time window so that streaming is more observable
    on pbi

    Sorting is done to make sure events are not dropped due to coincidental late event
    showing up before the earlier one
    """
    df = pd.read_parquet(path)
    df = df[df["tpep_pickup_datetime"].dt.strftime("%Y-%m") == year_month]
    if day is not None:
        df = df[df["tpep_pickup_datetime"].dt.day == day]
    if start_hour is not None:
        df = df[df["tpep_pickup_datetime"].dt.hour >= start_hour]
    if end_hour is not None:
        df = df[df["tpep_pickup_datetime"].dt.hour < end_hour]
    sample = df.sample(n=min(n, len(df))).sort_values("tpep_pickup_datetime")
    return sample.to_dict(orient="records")


def stream_trips(producer, trips, topic="taxi-trips-raw", delay_seconds=0.05):
    # delay_seconds to prevent everything arriving at kafka at the same time
    for i, trip in enumerate(trips):
        producer.send(topic, value=trip)
        if (i + 1) % 100 == 0:
            logger.info(f"Sent {i + 1}/{len(trips)} trips")
        if delay_seconds > 0:
            time.sleep(delay_seconds)
    producer.flush()


def send_malformed_samples(topic="taxi-trips-raw"):
    """Send malformed json to simulate broken message being sent to kafka's dead letter queue.
    NYC TLC publish the data in parquet format that can't contain malformed json. 

    json object: {"PULocationID": 121, "fare_amount": 18}, {}
    json string: "hello", "{not a json"

    As the sample get serialized, they are converted into a json string, 
    which from_json (in ingest_streaming) parse into a struct of nulls.
    """
    raw_producer = get_producer()
    broken = [
        "{not a json",
        "not a json",
        "{{{",
    ]
    for payload in broken:
        raw_producer.send(topic, value=payload)
    raw_producer.flush()
    raw_producer.close()
    logger.info(f"Sent {len(broken)} malformed samples")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Replay one month of raw taxi trips into Kafka"
    )
    parser.add_argument(
        "--year-month",
        required=True,
        help="Month to replay, formatted YYYY-MM",
    )
    parser.add_argument(
        "--day", type=int,
        help="Which day to replay, formatted DD",
    )
    parser.add_argument(
        "--start-hour", type=int,
        help="What start hour, formatted HH",
    )
    parser.add_argument(
        "--end-hour", type=int,
        help="What end hour, formatted HH",
    )
    parser.add_argument(
        "--n", type=int, default=1000,
        help="How many trips to sample and send (default 1000)",
    )
    parser.add_argument(
        "--delay-seconds", type=float, default=0.05,
        help="Pause between messages, for demo visibility (default 0.05)",
    )
    parser.add_argument(
        "--include-malformed", action="store_true",
        help="Also send a few unparseable messages, to exercise the dead-letter path",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    producer = get_producer()
    path = raw_path_for_month(args.year_month)

    logger.info(f"Loading raw trips for {args.year_month} from {path}")
    trips = load_trips(
        path,
        args.year_month,
        day=args.day,
        start_hour=args.start_hour,
        end_hour=args.end_hour,
        n=args.n
    )
    logger.info(f"Loaded {len(trips)} trips to stream")

    if args.include_malformed:
        send_malformed_samples()

    stream_trips(producer, trips, delay_seconds=args.delay_seconds)
    logger.info("Finished streaming all trips")


if __name__ == "__main__":
    main()

# run locally (not in a container):
# python src/producer.py --year-month 2026-03 --n 1000 --day 15 --start-hour 08 --end-hour 11 --include-malformed
