# syntax=docker/dockerfile:1.7
# One image backs the master, the workers and the driver, so every Spark
# process in the stack runs the same Spark, Scala and Python versions.
FROM apache/spark:4.1.4-python3

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

COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf

USER spark
