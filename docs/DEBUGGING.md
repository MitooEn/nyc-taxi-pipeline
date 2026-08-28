# Notable bugs

---

## 1. `from_json` treats a type mismatch as a parse failure

**Symptom**: Every valid trip routed to the dead-letter topic while bronze stayed
empty.

**Cause**: `from_json` runs in PERMISSIVE mode by default which mean unparsable message become
struct of nulls instead of null struct. This is indistinguishable from a proper message carrying no values.
Declaring `columnNameOfCorruptRecord` allows spark to write the raw message of the unparsable message into it.
This also helps in differentiating struct of nulls and genuine empty message.

**Symptom**: However, `_corrupt_record` was firing on messages that parsed correctly. A temporary 
script showed records with `is_corrupt=true` *and* a correctly parsed `PULocationID` which suggest
a type mismatch instead of malformed JSON.

**Cause**: pandas cannot hold nulls in `int64`, so all `int64` column with nulls become
`float64`. These column are`passenger_count` and `RatecodeID` confirmed with `df.dtypes`. So, `json.dumps` wrote
`1.0` instead of `1` and when checked against `LongType`, Spark fail the entire row.

**Fix**: Accept `DoubleType` and cast back to LongType before writing
bronze. However, we have to convert `NaN` to `None` before casting to `LongType`.
In an earlier attempt, directly casting `NaN` to `LongType` raises `CAST_OVERFLOW`

Avro or Protobuf with a schema registry would be ideal here, which
makes this class of mismatch impossible rather than something the consumer has to
tolerate.

---

## 2. A streaming file sink and a batch write cannot share a directory

**Symptom**: `cleaning.py` reported "no bronze rows found" for January, while the
January partitions were visibly present on disk.

**Cause**: A Spark file-sink streaming write creates a `_spark_metadata` log in its output
directory. The log contains a record of what file Spark created, hence another job without spark
metadata will be ignored as it's not recognized.

**Fix**: split bronze into `bronze/batch/` and `bronze/streaming/`.

---

## 3. Airflow UID mismatch

**Symptom**: `ingest_bronze` fails, with the log filled by warnings before it:

```
WARN FileUtil: Failed to delete file or dir
  [/data/bronze/taxi_trips/._SUCCESS.crc]: it still exists.
WARN FileUtil: Failed to delete file or dir
  [/data/bronze/taxi_trips/pickup_date=2026-01-01/part-00000-...]
```

Repeated for every file. Logged at WARN, so the job ran past them and failed
later when it tried to write into a directory it had not managed to clear.

Confusing because the files were visibly on disk and reading them worked. Only
deletion failed.

**Cause**: ownership. Confirmed directly:

```
docker exec spark-master ls -la /data/bronze/taxi_trips/
drwxr-xr-x 1 spark spark ...

docker exec airflow-scheduler id
uid=50000(airflow) gid=0(root)
```

Files owned by `spark` (UID 185) while the process running as `airflow` (UID 50000).
Directories are `drwxr-xr-x`, so only the owner can delete within them and
`mode("overwrite")` deletes before writing.

During development, the batching scripts were run manually through `spark-master`
so everything runs properly with UID 185.

This happen in Airflow because `spark-submit` runs in client mode,
so the Spark driver is a process inside whichever container launched it. In this case,
it's run by `SparkSubmitOperator` through Airflow container with UID 50000 conflicting with
UID 185.

**Fix**: `user: "185:0"` on all five Airflow services. Moving Airflow to Spark's
UID rather than the reverse because the Airflow image is built to run
under an arbitrary UID with GID 0 while the `apache/spark` image is not.

---

## 4. Caching the silver input in `aggregate.py`

Counting `InMemoryFileIndex` lines in a job's log show silver is read **eight times** per run.
Twice for each table, each once by `check_non_empty` and  once by `write_gold`. Use `.cache()`
to read silver once and store df in memory. However, it still measures 3–6 seconds slower.

The Storage tab ruled out the possibility of spillover to disk because the full 378 MB cached in
memory (1 GB worker). So it's the overhead cost itself that causes
it to be slower. For small data, re-reading everything is cheaper.

`.cache()` is kept, on the basis that read cost grows faster with data volume than
the caching overhead does, so this should invert at a larger scale.

---

## 5. JDBC driver classpath

**Symptom**: Spark's image doesn't have JDBC driver which is why we
download JDBC.jar and point it directly to Spark. However, an error occur: 
`ClassNotFoundException: com.clickhouse.jdbc.ClickHouseDriver`,
despite the JAR resolving and demonstrably containing that class.

A Spark application has a **driver** JVM and **executor** JVMs, each with its own
classpath set by different flags.

**Cause**: ` --jars` ships the JAR to executors, which covered the write. But the
`DROP PARTITION` call uses `spark._jvm` directly which runs in the driver's JVM.
However, `--jars` is only for executors. Drivers use `--driver-class-path`.

So one flag for half the script and left the other half failing, which is why
it looked like the JAR was simultaneously present and missing.


**Fix**:

1. **Supplying the file directly with `--jars` and `--driver-class-path`**
because `Class.forName` still could not see the Jar when Ivy's was used to resolve
the path.

2. **Mount in `docker-compose.yml` with `/tmp/jars`** instead of `/opt/spark/jars`.
Bind mounts *replace* instead of merge, so mounting with `/opt/spark/jars` cause Spark's
pre-built Jar files to be masked by our single Jar file.

3. **`./jars` mount lives on the Airflow services alone** because
`load_clickhouse.py` only runs as a DAG task. The JAR must be visible wherever
`spark-submit` runs. Solving it for the Spark containers did not solve it for Airflow,
which runs `spark-submit` itself under `SparkSubmitOperator`.

---

## 6. Bug on Kafka-ClickHouse path that fail with unclear error message

**Symptom**: Every row failed at parse time: offsets advanced, the table stayed empty, and the only
trace was `Skipped 1 rows with errors` in `clickhouse-server.log`

**Cause**: ClickHouse's `DateTime` parser rejects ISO 8601. Spark's `to_json` outputs
`"2026-03-16T08:20:00.000Z"` while ClickHouse expects `2026-03-16 08:20:00`. 

**Fix**: read the timestamps as `String` in the queue table and convert with
`parseDateTimeBestEffort` in the materialized view.

