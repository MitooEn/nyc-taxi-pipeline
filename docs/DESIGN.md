# Design

## Batch path

Four Airflow tasks, each processing one month: `ingest_bronze -> clean_silver -> 
aggregate_gold -> load_clickhouse`.

### Parameterization

Every script takes `--year-month` which change dynamically via a Jinja
template rendered at task runtime:

```python
TARGET_MONTH = "{{ data_interval_start.strftime('%Y-%m') }}"
```

each interval passes its own month.

### Write mode
`spark.sql.sources.partitionOverwriteMode = "dynamic"` it uses `dynamic` in every script.
Without it, `mode("overwrite")` rewrite the entire directory; e.g.: writing February
would delete January instead of incrementing February to January.

### Partition pruning
`cleaning.py` and `aggregate.py` filter to the target month by comparing
`pickup_date` against a date range rather than applying `date_format()`.
This is because `date_format()` map DateType to string.
Spark can't map the converted data back for partition pruning.


### Bronze

Raw Parquet read against an explicit `StructType`, so a renamed column surfaces at read time.

A zero-row guardrail exists to prevent Spark from writing an empty DataFrame.

### Silver

**Cleaning rules:**

| Rule | Reasoning |
|---|---|
| Filter `pickup_date` to target month | Monthly TLC files carry boundary-spillover trips from adjacent months. |
| Drop exact duplicates | Removes repeated records. |
| Drop rows with null `tpep_pickup_datetime`, `fare_amount`, or `total_amount` | Crucial for every downstream aggregation. |
| Fill null `mta_tax` -> `0.50`, `improvement_surcharge`-> `1.00` | Fixed standardized charges. |
| Fill null `extra`, `tolls_amount`, `congestion_surcharge`, `Airport_fee`, `cbd_congestion_fee` -> `0` | Situational fees. |
| Fill null `tip_amount` -> `0` | The field only captures credit card tips, cash tips are not recorded at all. Inherent issue. |
| Fill null `passenger_count` -> `1` | ~29% of rows had null `passenger_count`. Imputed to the minimum possible value instead of discarding the rows. |
| Filter `passenger_count > 0` | Excludes zero passenger record. |
| Filter `0 < trip_distance <= 100` | Excludes zero-distance and implausibly long trips. |
| Filter `tpep_dropoff_datetime > tpep_pickup_datetime` | Excludes zero trip duration record. |
| Filter `PULocationID` / `DOLocationID` within `1–263` | TLC's documented valid zone range. |
| Filter `fare_amount >= 0`, `total_amount >= 0` | Excludes negative fares. |

An initial `passenger_count > 0` filter dropped ~29% of rows. Splitting the
count into `isNull()` versus `== 0` showed almost all were nulls rather than
zeros. This led to imputation instead of exclusion and took the overall
drop rate from 33% to 6%.

**Guardrail**: the job fails if the drop rate exceeds 10%.

### Gold

Four summaries, each partitioned by `year_month`:

1. **Daily** -- trip count, revenue, average fare per day.
2. **Hourly** -- trip count, average fare, average tip by hour of day.
3. **Pickup location** -- volume and averages per zone, joined against TLC's zone
   lookup for readable names.
4. **Borough** -- the same metrics as Pickup location but up to borough level.

`.cache()` is used on read_silver to *save* df in memory because it feeds all four
summaries and each is read twice (once by `check_non_empty` and  once by `write_gold`).
Spark DataFrames are lazy, so every action re-executes the plan from source causing
eight scans per run without a cache.

`.cache()` was measured 3–6 seconds slower here, with the full 378 MB held in memory and no
spill, so it is not a memory constraint. `.cache()` is kept on the basis that read cost grows
faster with volume than caching overhead does, which is untested. 
See [DEBUGGING.md](DEBUGGING.md) for more information.

### ClickHouse load

Reads a month from gold and writes it over JDBC. Idempotent via `ALTER TABLE ... DROP PARTITION ...`
before insert. This was chosen over `ALTER TABLE ... DELETE WHERE ...` because our ClickHouse tables
are `PARTITION BY year_month`, so dropping a month is a metadata operation instead of a row-rewriting mutation.

Spark's DataFrame writer offers no delete mode, so `DROP PARTITION` is issued as
a plain JDBC statement from the Spark driver rather than through the writer.
That is what forced the two-flag classpath fix described in [DEBUGGING.md](DEBUGGING.md).

---

## Orchestration

The batch path runs as four Airflow tasks while the streaming jobs run as
a service.

### DAG catchup and max_active_runs
The DAG runs with `catchup=False` to prevent months with no data from running
when DAG is triggered manually through backfilling. `max_active_runs=1` is set 
to due to memory constraint. Computer with more memory for Spark can remove this.

### Custom airflow image

`SparkSubmitOperator` runs `spark-submit` from inside the Airflow container, so
that image needs Java and Spark. The base `apache/airflow` image has neither.

To be more specific, Spark is not *executing* there. `spark-submit` is a client that
packages the job and hands it to `spark://spark-master:7077`, where the master
and worker do the actual computation. The Airflow container needs enough Spark
installed to make that handoff.

However, `spark-submit` runs in **client mode**, which means the Spark **driver** is an
Airflow-container process. Which means:

- The ClickHouse JDBC JAR has to be mounted into the Airflow containers instead of Spark's
  because `spark-submit` reads it from local disk wherever it runs and
  under SparkSubmitOperator that is Airflow.
- The Airflow services run as UID 185, matching the Spark image's UID, so files
  written to the shared `./data` volume have consistent ownership.
- A DAG task's Spark application UI binds inside the Airflow container, not
  `spark-master`, so inspecting a running task means publishing a port from the
  scheduler (port 4041:4040).

---

## Container networking

**Inspection of Spark task triggered by DAG** is done from (http://localhost:4041/).

**Published ports exist only for the host.** `8082:8080` lets a browser on
Windows and only on the specific Windows to reach the Airflow api-server hence the plaintext credentials
in the code. For a similar reason, Postgres (no mapping at all) also use plaintext credentials.

**Kafka uses two listeners** because it is addressed from both sides:
`kafka:9092` from containers and `localhost:29092` from `producer.py` (runs on
the host).

The same boundary caused failures documented in
[DEBUGGING.md](DEBUGGING.md): UID mismatches on the shared
`./data` bind mount.

---

## Streaming path

Two consumers of `taxi-trips-raw`, doing different jobs.
Both run as Compose services dependent on Kafka and
Spark master passing their healthchecks.

### The two consumers

Kafka is an append-only log. Each consumer tracks its own offset.
One event stream can serves several readers.

**`streaming_ingest.py`** parses events and appends them to a bronze.
Its data quality concern is structural where unparseable messages go
to a dead-letter topic. This job also simulate where live data usually
got written into bronze layer

**`streaming_analytics.py`** computes net flow per zone in sliding windows. Usage
of windowing and watermarking. This job simulate live data stream.


### Splitting bronze

`bronze/batch/` is written by `ingest.py` while `bronze/streaming/` by
`streaming_ingest.py`. They cannot share a directory because **Spark streaming**
write creates a `_spark_metadata` log that becomes authoritative for that path,
making any file not listed in it invisible to later Spark reads [DEBUGGING.md](DEBUGGING.md).

### Net flow

A pickup removes an available car from a zone; a dropoff adds one back. Each trip
becomes two events:

```
pickup_time,  PULocationID -> delta -1
dropoff_time, DOLocationID -> delta +1
```

Summed per zone per window. Negative means demand is drawing cars away which become
a signal for human watching a dashboard could scan for.

### Windowing and watermarking

15-minute windows sliding every 5 minutes. Sliding rather than tumbling gives a
fresh reading every 5 minutes instead of only at fixed boundaries. Ideally it should
slide more often (such as every 1-2 minutes) but due to author's memory constraint, 5 minutes
was chosen.

**The watermark is 45 minutes.** Both events from a trip arrive in the same message, 
but their event times differ by the trip duration. The watermark tracks `max(event_time)`,
so dropoffs push it forward while the pickup from that same message is already well
behind it. A short watermark would drop almost every pickup.

Kafka sink accepts `update` mode, so windows updates as they change. Meanwhile, a file sink
would only accept `append` and would have to wait 45 minutes since the window passed
before it can append its result.

### ClickHouse-Kafka

A Kafka-engine table, a `ReplacingMergeTree` table (storage), and a
materialized view moving rows between them. Created in that order because
view validates its `TO` target at creation, so storage must exist first. The
queue table begins consuming the moment it's created, so the gap before the view is
attached should be as small as possible as anything read in that gap is
discarded.

`ReplacingMergeTree` to accommodate for `update` mode. Deduplication happens
during background merges, so `FINAL` forces it at query time.

`zone_net_flow_named` joins the zone lookup to provide more readable data for human.

---

## Testing

`pytest` covers the batch pipeline's transformation logic, running
in a local `local[1]` SparkSession instead of than the Docker cluster.

- **`test_ingest.py`** -- path construction, `pickup_date` derivation, and the
  zero-row guardrail
- **`test_cleaning.py`** -- one or more tests per cleaning rule except `drop_duplicates`
  and the drop-rate guardrail
- **`test_aggregate.py`** -- each summary builder and ensuring left join
  keeps trips with an unmatched location ID

**A defined schema** is used because several cleaning tests construct a single-row
DataFrame where the one column under test is `None`. Without an explicit schema,
Spark has no non-null value to infer that column's type from.

**Test schema are narrow** as tests include only the columns each function
reads or modifies.

**The streaming jobs have no tests** as their testable functions are few in number and the
parts most likely to break: parser behaviour and watermark semantics are the
hard to test (especially watermark). Instead, bronze data was verified by a manual inspection
tool: `read_parquet_bronze.py`,
DLQ by:

```powershell
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --topic taxi-trips-dlq --bootstrap-server localhost:9092 --from-beginning --timeout-ms 5000
```

and live streaming observed through PowerBI
