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
COPY analytics/pipelines/pyproject.toml analytics/pipelines/
RUN --mount=from=ghcr.io/astral-sh/uv:0.13.7,source=/uv,target=/usr/local/bin/uv \
    uv sync --frozen --package pipelines --group dev --no-install-workspace

COPY analytics/ analytics/
COPY tests/ tests/
RUN --mount=from=ghcr.io/astral-sh/uv:0.13.7,source=/uv,target=/usr/local/bin/uv \
    uv sync --frozen --package pipelines --group dev \
    && python3.14 -m zipfile -c /opt/app/packages.zip \
        analytics/common/src/common analytics/pipelines/src/pipelines

# The image's own PySpark, so plain python and pytest import the cluster's exact Spark version.
ENV PATH=/opt/venv/bin:$PATH \
    PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.9-src.zip \
    PYSPARK_DRIVER_PYTHON=/opt/venv/bin/python \
    PYTHONDONTWRITEBYTECODE=1

USER spark
