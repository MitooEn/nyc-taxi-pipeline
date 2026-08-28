from datetime import datetime

import pytest

from src.ingest import raw_path_for_month, add_pickup_date, check_non_empty


def test_raw_path_for_month_builds_expected_filename():
    assert raw_path_for_month("2026-01") == "/data/raw/yellow_tripdata_2026-01.parquet"


def test_raw_path_for_month_respects_custom_base_path():
    assert raw_path_for_month("2026-02", base_path="/tmp/raw") == \
        "/tmp/raw/yellow_tripdata_2026-02.parquet"


def test_add_pickup_date_handles_end_of_day(spark):
    data = [(1, datetime(2026, 1, 31, 23, 59, 59))]
    df = spark.createDataFrame(data, ["VendorID", "tpep_pickup_datetime"])
    result = add_pickup_date(df)
    assert str(result.collect()[0]["pickup_date"]) == "2026-01-31"


def test_check_non_empty_returns_count():
    assert check_non_empty(3000000) == 3000000


def test_check_non_empty_raises_on_zero():
    with pytest.raises(ValueError):
        check_non_empty(0)