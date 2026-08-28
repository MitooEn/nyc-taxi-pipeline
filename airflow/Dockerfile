FROM apache/airflow:3.3.0

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jdk-headless curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Download Spark's prebuilt binary distribution from Apache's CDN rather than
# pip-installing pyspark. The PyPI package is a 455 MB source tarball that must
# be built during install; this is a prebuilt archive that only needs extracting.
ENV SPARK_VERSION=4.1.2
RUN curl -fSL "https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" \
      -o /tmp/spark.tgz \
    && tar -xzf /tmp/spark.tgz -C /opt \
    && mv "/opt/spark-${SPARK_VERSION}-bin-hadoop3" /opt/spark \
    && rm /tmp/spark.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"

USER airflow

RUN pip install --no-cache-dir --timeout 120 --retries 10 \
    apache-airflow-providers-apache-spark

# Create: docker build -t airflow-spark:3.3.0 ./airflow
# Verify: docker run --rm --entrypoint spark-submit airflow-spark:3.3.0 --version