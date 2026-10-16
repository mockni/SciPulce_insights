# =============================================================================
#  Image Spark SciPulse (master + workers du cluster standalone)
#  Python 3.11 + PySpark 3.5.3 : MÊME version de Python que le driver (conteneurs
#  Airflow), condition imposée par PySpark pour les UDF (TF-IDF, top-k).
# =============================================================================
FROM python:3.11-slim-bookworm

ARG PYSPARK_VERSION=3.5.3
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps curl tini \
 && rm -rf /var/lib/apt/lists/* \
 && ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /opt/java

RUN pip install --no-cache-dir "pyspark==${PYSPARK_VERSION}" "numpy>=1.26,<2"

# Connecteurs : S3A (MinIO) et elasticsearch-hadoop
COPY download-jars.sh /tmp/download-jars.sh
RUN sh /tmp/download-jars.sh /opt/spark-jars

ENV JAVA_HOME=/opt/java \
    SPARK_HOME=/usr/local/lib/python3.11/site-packages/pyspark \
    PYSPARK_PYTHON=python3 \
    SPARK_NO_DAEMONIZE=true
ENV PATH="${SPARK_HOME}/bin:${PATH}"

ENTRYPOINT ["/usr/bin/tini", "--"]
