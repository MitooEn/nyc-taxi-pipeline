from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    "owner": "me",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

"""Jinja templating
{{ data_interval_start.strftime('%Y-%m') }} -> start of the data interval.
Substitute content to start of the data interval 
(in this case the first day of each month) for every task run
"""
TARGET_MONTH = "{{ data_interval_start.strftime('%Y-%m') }}"

"""ClickHouse JDBC driver isn't prebuilt into spark's image, so it is
mounted in from ./jars and passed at submit time.
"""
CLICKHOUSE_JAR = "/tmp/jars/clickhouse-jdbc-0.9.7-all.jar"

with DAG(
    dag_id="taxi_batch_pipeline",
    description="Bronze -> silver -> gold -> ClickHouse batch pipeline for NYC taxi trip data",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="@monthly",
    # catchup=False -> process only this month's data
    catchup=False,
    # max_active_runs= -> resource management (only 1gb for spark worker)
    max_active_runs=1,
    tags=["spark", "batch", "taxi", "clickhouse"],
) as dag:

    ingest = SparkSubmitOperator(
        task_id="ingest_bronze",
        application="/src/ingest.py",
        application_args=["--year-month", TARGET_MONTH],
        conn_id="spark_default",
    )

    cleaning = SparkSubmitOperator(
        task_id="clean_silver",
        application="/src/cleaning.py",
        application_args=["--year-month", TARGET_MONTH],
        conn_id="spark_default",
    )

    aggregate = SparkSubmitOperator(
        task_id="aggregate_gold",
        application="/src/aggregate.py",
        application_args=["--year-month", TARGET_MONTH],
        conn_id="spark_default",
    )

    # jars -> point JDBC to spark executor
    # driver_class_path -> point JDBC to spark driver (connection via spark jvm)
    load_clickhouse = SparkSubmitOperator(
        task_id="load_clickhouse",
        application="/src/load_clickhouse.py",
        application_args=["--year-month", TARGET_MONTH],
        conn_id="spark_default",
        jars=CLICKHOUSE_JAR,
        driver_class_path=CLICKHOUSE_JAR,
    )

    ingest >> cleaning >> aggregate >> load_clickhouse