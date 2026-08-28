from datetime import datetime

import pytest

from src.aggregate import (
    build_daily_summary,
    build_hourly_summary,
    build_location_summary,
    build_borough_summary,
    check_non_empty,
)

TRIP_COLUMNS = [
    "pickup_date", "tpep_pickup_datetime", "PULocationID",
    "fare_amount", "tip_amount", "total_amount",
]
ZONE_COLUMNS = ["LocationID", "Borough", "Zone", "service_zone"]


def make_trip(day, hour, pu_location_id, fare_amount,
              tip_amount=2.0, total_amount=None, month=1):
    return (
        datetime(2026, month, day).date(),
        datetime(2026, month, day, hour, 0, 0),
        pu_location_id,
        fare_amount,
        tip_amount,
        total_amount if total_amount is not None else fare_amount + tip_amount,
    )


@pytest.fixture
def zone_lookup(spark):
    # A small simulated taxi zone lookup table
    data = [
        (100, "Manhattan", "Midtown Center", "Yellow Zone"),
        (200, "Queens", "JFK Airport", "Airports"),
        (300, "Brooklyn", "Williamsburg", "Boro Zone"),
    ]
    return spark.createDataFrame(data, ZONE_COLUMNS)


def test_build_daily_summary_groups_by_date(spark):
    data = [
        make_trip(15, 10, 100, 20.0),
        make_trip(15, 14, 100, 30.0),
        make_trip(16, 9, 100, 15.0),
    ]
    df = spark.createDataFrame(data, TRIP_COLUMNS)
    result = build_daily_summary(df)
    assert result.count() == 2
    jan_15 = result.filter(result.pickup_date == datetime(2026, 1, 15).date()).collect()[0]
    assert jan_15["trip_count"] == 2


def test_build_daily_summary_computes_correct_revenue(spark):
    data = [
        make_trip(15, 10, 100, 20.0, tip_amount=0.0, total_amount=20.0),
        make_trip(15, 14, 100, 30.0, tip_amount=0.0, total_amount=30.0),
    ]
    df = spark.createDataFrame(data, TRIP_COLUMNS)
    row = build_daily_summary(df).collect()[0]
    assert row["total_revenue"] == 50.0
    assert row["avg_fare"] == 25.0


def test_build_hourly_summary_groups_multiple_dates_same_hour(spark):
    data = [make_trip(15, 8, 100, 20.0), make_trip(16, 8, 100, 25.0)]
    df = spark.createDataFrame(data, TRIP_COLUMNS)
    result = build_hourly_summary(df)
    assert result.count() == 1
    assert result.collect()[0]["trip_count"] == 2


def test_build_location_summary_orders_by_trip_count_descending(spark, zone_lookup):
    data = [
        make_trip(15, 10, 100, 20.0),
        make_trip(15, 11, 200, 20.0),
        make_trip(15, 12, 200, 20.0),
        make_trip(15, 13, 200, 20.0),
    ]
    df = spark.createDataFrame(data, TRIP_COLUMNS)
    rows = build_location_summary(df, zone_lookup).collect()
    assert rows[0]["PULocationID"] == 200
    assert rows[0]["trip_count"] == 3


def test_build_location_summary_attaches_correct_zone_name(spark, zone_lookup):
    df = spark.createDataFrame([make_trip(15, 10, 100, 20.0)], TRIP_COLUMNS)
    row = build_location_summary(df, zone_lookup).collect()[0]
    assert row["pickup_zone_name"] == "Midtown Center"
    assert row["pickup_borough"] == "Manhattan"


def test_build_location_summary_keeps_trip_with_unmatched_location_id(spark, zone_lookup):
    df = spark.createDataFrame([make_trip(15, 10, 999, 20.0)], TRIP_COLUMNS)
    result = build_location_summary(df, zone_lookup)
    assert result.count() == 1
    row = result.collect()[0]
    assert row["pickup_zone_name"] is None
    assert row["pickup_borough"] is None


def test_build_borough_summary_aggregates_by_borough(spark, zone_lookup):
    data = [
        make_trip(15, 10, 100, 20.0),  # Manhattan
        make_trip(15, 11, 100, 30.0),  # Manhattan
        make_trip(15, 12, 200, 50.0),  # Queens
    ]
    df = spark.createDataFrame(data, TRIP_COLUMNS)
    rows = {r["Borough"]: r for r in build_borough_summary(df, zone_lookup).collect()}
    assert rows["Manhattan"]["trip_count"] == 2
    assert rows["Queens"]["trip_count"] == 1


def test_build_borough_summary_orders_by_trip_count_descending(spark, zone_lookup):
    data = [
        make_trip(15, 10, 100, 20.0),
        make_trip(15, 11, 100, 20.0),
        make_trip(15, 12, 100, 20.0),
        make_trip(15, 13, 200, 20.0),
    ]
    df = spark.createDataFrame(data, TRIP_COLUMNS)
    assert build_borough_summary(df, zone_lookup).collect()[0]["Borough"] == "Manhattan"


def test_check_non_empty_returns_row_count(spark):
    df = spark.createDataFrame([(1,), (2,), (3,)], ["id"])
    assert check_non_empty(df, "test_df") == 3


def test_check_non_empty_raises_on_empty_dataframe(spark):
    df = spark.createDataFrame([], "id: int")
    with pytest.raises(ValueError):
        check_non_empty(df, "test_df")