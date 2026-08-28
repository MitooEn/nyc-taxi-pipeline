# Limitations

---

## The source data is particularly clean

NYC TLC publishes processed, schema-conformed Parquet. Parquet also cannot hold 
malformed data, so every value conforms to its column type or the file would be invalid.

So "raw" here means raw relative to this pipeline, not raw as a source system
would produce. Genuine event data would carry truncation, encoding problems,
duplicate sends, and format drift that Parquet cannot represent.

Two consequences:

**The dead-letter path has to be triggered artificially.** `producer.py` injects
three unparseable payloads.

**The watermark is never actually exercised.** The producer sorts trips by pickup
time, which is required for the simulation to work as randomly ordered events
spanning a month would push the watermark hours ahead and cause mass drops. But
sorted arrival means nothing is ever late.

---

## The streaming output has no consumer beyond the dashboard

`zone-net-flow` lands in ClickHouse and Power BI reads it, so the path terminates
somewhere queryable. But there is no *ideal end* such as dispatch service or alerting
system acting on the data. The current consumer is a human watching a dashboard, which is
a valid use case but a modest one.

Additionally, dispatch service, alerting system, or automated consumer
would want sub-second latency and a key-value store. That would be Flink writing to
Redis, not Spark writing to ClickHouse.

---

## JSON instead of a typed format

Every type mismatch in [DEBUGGING.md](DEBUGGING.md) traces back to JSON having
four basic datatypes (String, Number, Boolean, Null) and everything else have to be encoded 
as one of them and reconstructed on the other side.

Avro or Protobuf with a schema registry would solve this issue, and would enforce
compatibility checks when a producer's schema changes.

---

## Credentials are in plaintext

Postgres and ClickHouse credentials are committed in `docker-compose.yml`, and
Airflow's UI has authentication disabled entirely via
`AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS`.

Acceptable here as it's only for local use. However, production environment
will require secrecy.

---

## Operational gaps

- **A single Kafka partition** on every topic means no consumer-group
  parallelism or key-based routing is exercised. `streaming_analytics.py` keys
  its output by `zone_id`, which would matter with more partitions but currently
  does nothing.
- **The Spark worker is a single node with 6 cores and 1 GB.** This inherently
  cause runtime issue. Author's laptop simply didn't have enough memory. For
  laptops/computer with more memory available, Spark should be allocated more
  cores and memory.

---

## Planned next steps

- Avro with a schema registry, replacing JSON.
- Flink as a third consumer of `taxi-trips-raw` for comparison with Spark analytics job
  or outright replacing it with Flink.
- A consumer for `zone-net-flow` beyond the dashboard.

