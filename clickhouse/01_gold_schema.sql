-- Supported data type docs:
-- https://clickhouse.com/docs/integrations/connectors/data-ingestion/apache-spark/spark-native-connector#inserting-data-from-spark-into-clickhouse

-- ClickHouse table definitions for the gold summaries.

CREATE TABLE IF NOT EXISTS daily_summary (
    year_month    String,
    pickup_date   Date,
    trip_count    Int64,
    total_revenue Float64,
    avg_fare      Float64
) ENGINE = MergeTree()
PARTITION BY (year_month)
ORDER BY (pickup_date);


CREATE TABLE IF NOT EXISTS hourly_summary (
    year_month  String,
    pickup_hour Int32,
    trip_count  Int64,
    avg_fare    Float64,
    avg_tip     Float64
) ENGINE = MergeTree()
PARTITION BY (year_month)
ORDER BY (pickup_hour);


CREATE TABLE IF NOT EXISTS pickup_location_summary (
    year_month       String,
    PULocationID     Int32,
    pickup_zone_name Nullable(String),
    pickup_borough   Nullable(String),
    trip_count       Int64,
    avg_fare         Float64,
    avg_tip          Float64
) ENGINE = MergeTree()
PARTITION BY (year_month)
ORDER BY (trip_count);


CREATE TABLE IF NOT EXISTS borough_summary (
    year_month String,
    Borough    Nullable(String),
    trip_count Int64,
    avg_fare   Float64,
    avg_total  Float64
) ENGINE = MergeTree()
PARTITION BY (year_month)
ORDER BY (trip_count);