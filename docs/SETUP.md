# Setup

## Prerequisites

### Folder Structure
```
nyc-taxi-pipeline/
├── README.md
├── docker-compose.yml
├── .gitignore
├── architecture.png
│
├── .github/
│   └── workflows/
│       └── tests.yml
│
├── docs/
│   ├── DESIGN.md
│   ├── SETUP.md
│   ├── DEBUGGING.md
│   └── LIMITATIONS.md
│
├── airflow/
│   ├── Dockerfile
│   ├── dags/
│   │   └── taxi_pipeline_dag.py
│   └── logs/                                     <- gitignored, create before first run
│
├── src/
│   ├── ingest.py
│   ├── cleaning.py
│   ├── aggregate.py
│   ├── load_clickhouse.py
│   ├── producer.py
│   ├── streaming_ingest.py
│   └── streaming_analytics.py
│
├── tools/
│   └── read_parquet_bronze.py
│
├── tests/
│   ├── conftest.py
│   ├── test_ingest.py
│   ├── test_cleaning.py
│   └── test_aggregate.py
│
├── clickhouse/
│   ├── 01_gold_schema.sql
│   └── 02_streaming_schema.sql
│
├── spark-conf/
│   └── spark-defaults.conf
│
├── jars/                                         <- gitignored
│   └── clickhouse-jdbc-0.9.7-all.jar
│
└── data/                                         <- gitignored
    ├── raw/
    │   ├── yellow_tripdata_2026-01.parquet
    │   ├── yellow_tripdata_2026-02.parquet
    │   └── yellow_tripdata_2026-03.parquet
    ├── reference/
    │   └── taxi_zone_lookup.csv
    ├── bronze/
    │   ├── batch/
    │   └── streaming/
    ├── silver/
    ├── gold/
    └── checkpoints/
```

---
### Data
Download Yellow Taxi Trip Records from
(https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page):

Put the data in such a directory:

`data/raw/yellow_tripdata_2026-01.parquet`

`data/raw/yellow_tripdata_2026-02.parquet`

`data/raw/yellow_tripdata_2026-03.parquet`

`data/reference/taxi_zone_lookup.csv`

---

### Dependencies
Download **JDBC driver.** `clickhouse-jdbc-0.9.7-all.jar`:
(https://github.com/ClickHouse/clickhouse-java/releases/tag/v0.9.7)

Put into:

`jars/clickhouse-jdbc-0.9.7-all.jar`

---

**Python packages** for the producer and tests:
```powershell
pip install kafka-python pandas pytest pyspark==4.1.2
```

---

Download PowerBI

---

## Create Kafka topics

```powershell
docker exec -it kafka bash
```
```bash
/opt/kafka/bin/kafka-topics.sh --create --topic taxi-trips-raw --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
/opt/kafka/bin/kafka-topics.sh --create --topic taxi-trips-dlq --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
/opt/kafka/bin/kafka-topics.sh --create --topic zone-net-flow --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
/opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
exit
```

## Running the batch path

### 1. Run the DAG

Through Airflow (http://localhost:8082/): trigger a **Backfill** over `2026-01-01` to `2026-03-01`. One run at a time (`max_active_runs=1`).

### 2. Verify result in PowerBI

Get data from Clickhouse with credentials as:

| Field | Value |
|---|---|
| Host | `localhost` |
| Port | `8123` |
| Database | `default` |
| Username | `user1` |
| Password | `pass1` |

**Import** the data and build the chart

---

## Running the streaming path

### 1. Start producer

Ingest and analytics are already running as a service. Only the producer is manual, and it
runs on the host instead of a container because `kafka` does not resolve from
Windows, so it connects to the EXTERNAL listener through `localhost:29092`.

```powershell
python src/producer.py --year-month 2026-03 --n 1000 --day 15 --start-hour 08 --end-hour 11 --include-malformed
```

### 2. Observe live data in PowerBI

Get data from Clickhouse with credentials as:

| Field | Value |
|---|---|
| Host | `localhost` |
| Port | `8123` |
| Database | `default` |
| Username | `user1` |
| Password | `pass1` |

**DirectQuery** the data and build the chart. Turn on auto page refresh (APR) and observes as the chart changes. **Depending on how much
data were sampled, PowerBI might need to be initialized before producer**

### 3. Verify bronze_streaming data

```powershell
docker exec -it spark-master bash
```
```bash
/opt/spark/bin/spark-submit /tools/read_parquet_bronze.py
/opt/spark/bin/spark-submit /tools/read_parquet_bronze.py --show-partitions
```

Observe `null_pickup_date`, anything above zero means a
record that should have been dead-lettered reached bronze instead.

### 4. Check DLQ

Expect exactly the three unparseable payloads:

```powershell
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --topic taxi-trips-dlq --bootstrap-server localhost:9092 --from-beginning --timeout-ms 5000
```

---

## Resetting to clean state

### 1. Stop Docker

```powershell
docker compose down
```

---

### 2. Wipe local data

Delete `.\data\bronze`, `.\data\silver`, `.\data\gold`, `.\data\checkpoints`

Only `raw` and `reference` should remain. 

---

### 3. Reset Kafka topics

```powershell
docker compose up -d
```
```powershell
docker exec -it kafka bash
```
```bash
/opt/kafka/bin/kafka-topics.sh --delete --topic taxi-trips-raw --bootstrap-server localhost:9092
/opt/kafka/bin/kafka-topics.sh --delete --topic taxi-trips-dlq --bootstrap-server localhost:9092
/opt/kafka/bin/kafka-topics.sh --delete --topic zone-net-flow --bootstrap-server localhost:9092

sleep 10

/opt/kafka/bin/kafka-topics.sh --create --topic taxi-trips-raw --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
/opt/kafka/bin/kafka-topics.sh --create --topic taxi-trips-dlq --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
/opt/kafka/bin/kafka-topics.sh --create --topic zone-net-flow --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

/opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

---

### 4. Truncate ClickHouse

Leave `taxi_zone_lookup` as it is a static reference.

```powershell
docker exec clickhouse clickhouse-client --user user1 --password pass1 --query "TRUNCATE TABLE zone_net_flow"
docker exec clickhouse clickhouse-client --user user1 --password pass1 --query "TRUNCATE TABLE daily_summary"
docker exec clickhouse clickhouse-client --user user1 --password pass1 --query "TRUNCATE TABLE hourly_summary"
docker exec clickhouse clickhouse-client --user user1 --password pass1 --query "TRUNCATE TABLE pickup_location_summary"
docker exec clickhouse clickhouse-client --user user1 --password pass1 --query "TRUNCATE TABLE borough_summary"
```

---

## Tests

```powershell
pytest tests/ -v
```

Runs against a local `local[1]` SparkSession, so `docker compose up` is not
required.
