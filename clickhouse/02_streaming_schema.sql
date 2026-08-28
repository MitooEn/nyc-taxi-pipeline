-- Supported data type docs:
-- https://clickhouse.com/docs/integrations/connectors/data-ingestion/apache-spark/spark-native-connector#inserting-data-from-spark-into-clickhouse

-- ClickHouse ingestion of the streaming analytics output.
-- Kafka topic -> kafka engine -> mv -> target table

-- ReplacingMergeTree to update data (happen asynchronously)
CREATE TABLE IF NOT EXISTS zone_net_flow (
    window_start  DateTime,
    window_end    DateTime,
    zone_id       Int32,
    taxi_net_flow Int32
) ENGINE = ReplacingMergeTree()
PARTITION BY toDate(window_start)
ORDER BY (window_start, zone_id);


-- queue table with kafka engine acts as a stream connector between kafka and clickhouse
-- Read row only once and move on to next row
-- Timestamps are string here because Spark's to_json outputs datetime with ISO 8601 format
-- ("2026-03-16T08:20:00.000Z"), which clickHouse's datetime parser rejects
-- (expects: "2026-03-16 08:20:00").
CREATE TABLE IF NOT EXISTS zone_net_flow_queue (
    window_start  String,
    window_end    String,
    zone_id       Int32,
    taxi_net_flow Int32
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'zone-net-flow',
    kafka_group_name = 'clickhouse_net_flow',
    kafka_format = 'JSONEachRow';


-- A materialized view in clickHouse is a trigger, rows inserted into
-- the source table are transformed and written to the target.
-- parseDateTimeBestEffort handles the ISO 8601 format Spark produces, which is
-- why the queue table use raw strings and the storage table gets real datetime values.

-- mv is created last because it need source and target
CREATE MATERIALIZED VIEW IF NOT EXISTS zone_net_flow_mv
TO zone_net_flow
AS SELECT
    parseDateTimeBestEffort(window_start) AS window_start,
    parseDateTimeBestEffort(window_end)   AS window_end,
    zone_id,
    taxi_net_flow
FROM zone_net_flow_queue;


-- Static reference
CREATE TABLE IF NOT EXISTS taxi_zone_lookup (
    LocationID   Int32,
    Borough      String,
    Zone         String,
    service_zone String
) ENGINE = MergeTree()
ORDER BY (LocationID);

-- Idempotent: truncate before inserting, so reapplying
-- this file does not accumulate duplicate zones.
TRUNCATE TABLE taxi_zone_lookup;

INSERT INTO taxi_zone_lookup
SELECT * FROM file('/var/lib/clickhouse/user_files/reference/taxi_zone_lookup.csv', CSVWithNames);


-- FINAL -> force ReplacingMergeTree deduplication
-- View is only created wwhen we query it
CREATE OR REPLACE VIEW zone_net_flow_named AS
SELECT
    n.window_start,
    n.window_end,
    n.zone_id,
    z.Zone    AS zone_name,
    z.Borough AS borough,
    n.taxi_net_flow
FROM zone_net_flow AS n FINAL
LEFT JOIN taxi_zone_lookup AS z ON n.zone_id = z.LocationID;
-- zone_name and borough will only have a value (not null) if n.zone_id = z.LocationID on that row