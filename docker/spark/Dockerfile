# syntax=docker/dockerfile:1.7
# Two targets: "spark" runs the master, workers and history server; "app" adds the pipeline
# package on top of it, so drivers and executors always share Spark, Java and Python versions.
FROM apache/spark:4.1.4-python3 AS spark

USER root
RUN mkdir -p /opt/spark/conf /opt/spark/spark-events \
    && chown spark:spark /opt/spark/spark-events

# Python 3.14 for every driver and executor; the base image's Python 3.10 is left to Spark's own scripts.
ENV UV_PYTHON_INSTALL_DIR=/opt/python
RUN --mount=from=ghcr.io/astral-sh/uv:0.13.7,source=/uv,target=/usr/local/bin/uv \
    uv python install 3.14.10 \
    && ln -s "$(uv python find --managed-python 3.14.10)" /usr/local/bin/python3.14
ENV PYSPARK_PYTHON=/usr/local/bin/python3.14

# Iceberg drivers
ADD --chmod=644 --checksum=sha256:3123c3798af642dd537230923548e013ef41cc28b6626e5225808cf5670a0f82 \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/1.12.0/iceberg-spark-runtime-4.1_2.13-1.12.0.jar \
    /opt/spark/jars/
ADD --chmod=644 --checksum=sha256:dd7e49ffa4ac9b215bed846581991259e3aec9bb2fa08d6cec029c46cd66d957 \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.12.0/iceberg-aws-bundle-1.12.0.jar \
    /opt/spark/jars/

# Postgresql drivers
ADD --chmod=644 --checksum=sha256:73914527305a40cce504b0d3d90b23caf565136912d607ae8ae7c5895512332c \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.14/postgresql-42.7.14.jar \
    /opt/spark/jars/

# Kafka source
ADD --chmod=644 --checksum=sha256:94f9514f64b319aa31c3b2061bf2fb75700f82e14845fda71337a13696855710 \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.13/4.1.4/spark-sql-kafka-0-10_2.13-4.1.4.jar \
    /opt/spark/jars/
ADD --chmod=644 --checksum=sha256:b641257bf720279865206a9cd8d69691c8004bc18e653829c6e1aa1d748c6716 \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.1.4/spark-token-provider-kafka-0-10_2.13-4.1.4.jar \
    /opt/spark/jars/
ADD --chmod=644 --checksum=sha256:7568b998572d256f0b7bc0afdc1b7a2588b8b08415c62ce314c864a6851ae9d9 \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.9.1/kafka-clients-3.9.1.jar \
    /opt/spark/jars/
ADD --chmod=644 --checksum=sha256:5273c8bc8c0dca2211175c0d27efbd7228efae99689aac001a8e1ef8e872e9ef \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.1/commons-pool2-2.12.1.jar \
    /opt/spark/jars/

# Avro files
ADD --chmod=644 --checksum=sha256:696c36bed6eee622e411b6889d7a849f12332761685c3d2947ec7bd4f756cf8f \
    https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.13/4.1.4/spark-avro_2.13-4.1.4.jar \
    /opt/spark/jars/

# S3A filesystem for plain files and checkpoints in the lake; the AWS SDK comes from the Iceberg bundle
ADD --chmod=644 --checksum=sha256:d2d24e6b715b5656b9d238bf13b6682a07e2004dd36aad74a73aeb854319f4d9 \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.2/hadoop-aws-3.4.2.jar \
    /opt/spark/jars/
ADD --chmod=644 --checksum=sha256:7bef0ea4efd5e0860bab63e08317040951edb51b72a5be734284c28b8324ab87 \
    https://repo1.maven.org/maven2/software/amazon/awssdk/s3-transfer-manager/2.54.17/s3-transfer-manager-2.54.17.jar \
    /opt/spark/jars/
ADD --chmod=644 --checksum=sha256:0b00ae3d55c3d1213ca47b5651dc4c2a0080fc9b6192dde3d53c3da6c1811bcb \
    https://repo1.maven.org/maven2/software/amazon/awssdk/apache-client/2.54.17/apache-client-2.54.17.jar \
    /opt/spark/jars/

COPY docker/spark/spark-defaults.conf /opt/spark/conf/spark-defaults.conf

USER spark


FROM spark AS app

USER root
WORKDIR /opt/app
ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_PYTHON=/usr/local/bin/python3.14 \
    UV_PYTHON_DOWNLOADS=never \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

COPY pyproject.toml uv.lock .python-version ./
COPY analytics/common/pyproject.toml analytics/common/
COPY analytics/api/pyproject.toml analytics/api/
COPY analytics/pipelines/pyproject.toml analytics/pipelines/
RUN --mount=from=ghcr.io/astral-sh/uv:0.13.7,source=/uv,target=/usr/local/bin/uv \
    uv sync --frozen --all-packages --group dev --no-install-workspace

COPY analytics/ analytics/
COPY tests/ tests/
RUN --mount=from=ghcr.io/astral-sh/uv:0.13.7,source=/uv,target=/usr/local/bin/uv \
    uv sync --frozen --all-packages --group dev \
    && python3.14 -m zipfile -c /opt/app/packages.zip \
        analytics/common/src/common analytics/pipelines/src/pipelines

# The image's own PySpark, so plain python and pytest import the cluster's exact Spark version.
ENV PATH=/opt/venv/bin:$PATH \
    PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.9-src.zip \
    PYSPARK_DRIVER_PYTHON=/opt/venv/bin/python \
    PYTHONDONTWRITEBYTECODE=1 \
    COVERAGE_FILE=/tmp/.coverage \
    RUFF_CACHE_DIR=/tmp/.ruff_cache

USER spark
