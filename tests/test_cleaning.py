from datetime import datetime

import pytest
from pyspark.sql.types import (
    StructType, StructField, TimestampType, LongType,
    DoubleType, IntegerType, DateType,
)

from src.cleaning import (
    filter_to_month,
    fill_optional_fees,
    drop_incomplete_core_fields,
    filter_invalid_trips,
    check_drop_rate,
)

TRIP_COLUMNS = [
    "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count",
    "trip_distance", "PULocationID", "DOLocationID", "fare_amount",
    "total_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
    "improvement_surcharge", "congestion_surcharge", "Airport_fee",
    "cbd_congestion_fee", "pickup_date",
]

# Explicit schema so that spark.createDataFrame knows the exact schema
# otherwise it tries to infer which fail if there is only 1 row and the value at column is null
TRIP_SCHEMA = StructType([
    StructField("tpep_pickup_datetime", TimestampType(), True),
    StructField("tpep_dropoff_datetime", TimestampType(), True),
    StructField("passenger_count", LongType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("PULocationID", IntegerType(), True),
    StructField("DOLocationID", IntegerType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True),
    StructField("Airport_fee", DoubleType(), True),
    StructField("cbd_congestion_fee", DoubleType(), True),
    StructField("pickup_date", DateType(), True),
])


def make_valid_row(**overrides):
    row = {
        "tpep_pickup_datetime": datetime(2026, 1, 15, 10, 0, 0),
        "tpep_dropoff_datetime": datetime(2026, 1, 15, 10, 20, 0),
        "passenger_count": 1,
        "trip_distance": 5.0,
        "PULocationID": 100,
        "DOLocationID": 150,
        "fare_amount": 20.0,
        "total_amount": 25.0,
        "extra": 0.5,
        "mta_tax": 0.5,
        "tip_amount": 3.0,
        "tolls_amount": 0.0,
        "improvement_surcharge": 1.0,
        "congestion_surcharge": 0.0,
        "Airport_fee": 0.0,
        "cbd_congestion_fee": 0.0,
        "pickup_date": datetime(2026, 1, 15).date(),
    }
    row.update(overrides)
    return tuple(row[c] for c in TRIP_COLUMNS)


def make_df(spark, rows):
    return spark.createDataFrame(rows, TRIP_SCHEMA)


def test_filter_to_month_keeps_target_month(spark):
    data = [
        make_valid_row(pickup_date=datetime(2026, 1, 1).date()),
        make_valid_row(pickup_date=datetime(2026, 1, 31).date()),
    ]
    df = make_df(spark, data)
    assert filter_to_month(df, "2026-01").count() == 2


def test_filter_to_month_excludes_previous_month_spillover(spark):
    data = [
        make_valid_row(pickup_date=datetime(2025, 12, 31).date()),
        make_valid_row(pickup_date=datetime(2026, 1, 15).date()),
    ]
    df = make_df(spark, data)
    result = filter_to_month(df, "2026-01")
    assert result.count() == 1
    assert str(result.collect()[0]["pickup_date"]) == "2026-01-15"


def test_filter_to_month_excludes_next_month_spillover(spark):
    data = [
        make_valid_row(pickup_date=datetime(2026, 2, 1).date()),
        make_valid_row(pickup_date=datetime(2026, 1, 15).date()),
    ]
    df = make_df(spark, data)
    assert filter_to_month(df, "2026-01").count() == 1


def test_filter_to_month_works_for_any_month(spark):
    data = [
        make_valid_row(pickup_date=datetime(2026, 1, 31).date()),
        make_valid_row(pickup_date=datetime(2026, 2, 14).date()),
        make_valid_row(pickup_date=datetime(2026, 3, 1).date()),
    ]
    df = make_df(spark, data)
    result = filter_to_month(df, "2026-02")
    assert result.count() == 1
    assert str(result.collect()[0]["pickup_date"]) == "2026-02-14"


def test_fill_optional_fees_imputes_null_passenger_count_to_one(spark):
    df = make_df(spark, [make_valid_row(passenger_count=None)])
    assert fill_optional_fees(df).collect()[0]["passenger_count"] == 1


def test_fill_optional_fees_fills_mta_tax_with_fixed_rate(spark):
    df = make_df(spark, [make_valid_row(mta_tax=None)])
    assert fill_optional_fees(df).collect()[0]["mta_tax"] == 0.5


def test_fill_optional_fees_fills_tip_amount_with_zero(spark):
    df = make_df(spark, [make_valid_row(tip_amount=None)])
    assert fill_optional_fees(df).collect()[0]["tip_amount"] == 0


def test_drop_incomplete_core_fields_drops_null_fare_amount(spark):
    df = make_df(spark, [make_valid_row(fare_amount=None)])
    assert drop_incomplete_core_fields(df).count() == 0


def test_drop_incomplete_core_fields_keeps_complete_rows(spark):
    df = make_df(spark, [make_valid_row()])
    assert drop_incomplete_core_fields(df).count() == 1


def test_filter_invalid_trips_drops_zero_passenger_count(spark):
    df = make_df(spark, [make_valid_row(passenger_count=0)])
    assert filter_invalid_trips(df).count() == 0


def test_filter_invalid_trips_drops_negative_fare(spark):
    df = make_df(spark, [make_valid_row(fare_amount=-10.0)])
    assert filter_invalid_trips(df).count() == 0


def test_filter_invalid_trips_drops_dropoff_before_pickup(spark):
    df = make_df(spark, [make_valid_row(
        tpep_pickup_datetime=datetime(2026, 1, 15, 10, 30, 0),
        tpep_dropoff_datetime=datetime(2026, 1, 15, 10, 0, 0),
    )])
    assert filter_invalid_trips(df).count() == 0


def test_filter_invalid_trips_drops_zero_duration_trip(spark):
    same_moment = datetime(2026, 1, 15, 10, 0, 0)
    df = make_df(spark, [make_valid_row(
        tpep_pickup_datetime=same_moment,
        tpep_dropoff_datetime=same_moment,
    )])
    assert filter_invalid_trips(df).count() == 0


def test_filter_invalid_trips_drops_out_of_range_location_id(spark):
    df = make_df(spark, [make_valid_row(PULocationID=999)])
    assert filter_invalid_trips(df).count() == 0


def test_filter_invalid_trips_keeps_valid_row(spark):
    df = make_df(spark, [make_valid_row()])
    assert filter_invalid_trips(df).count() == 1


def test_check_drop_rate_calculates_correctly():
    assert check_drop_rate(raw_count=100, clean_count=94, threshold_pct=10) == 6.0


def test_check_drop_rate_does_not_raise_below_threshold():
    assert check_drop_rate(raw_count=100, clean_count=95, threshold_pct=10) == 5.0


def test_check_drop_rate_raises_above_threshold():
    with pytest.raises(ValueError):
        check_drop_rate(raw_count=100, clean_count=50, threshold_pct=10)


def test_check_drop_rate_raises_on_empty_input():
    with pytest.raises(ValueError):
        check_drop_rate(raw_count=0, clean_count=0)