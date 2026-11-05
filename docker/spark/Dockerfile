# One image backs the master, the workers and the driver, so every Spark
# process in the stack runs the same Spark, Scala and Python versions.
FROM apache/spark:4.1.4-python3

USER root
RUN mkdir -p /opt/spark/conf /opt/spark/spark-events \
    && chown spark:spark /opt/spark/spark-events
COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf

USER spark
