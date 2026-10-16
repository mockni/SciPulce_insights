# =============================================================================
#  Image Airflow SciPulse
#  = image officielle
#  + Java 17 et les jars Spark (le driver Spark tourne dans le conteneur Airflow, mode client)
#  + un virtualenv contenant les dépendances du pipeline (dont PySpark 3.5.3)
# =============================================================================
FROM apache/airflow:2.9.3-python3.11

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl \
 && rm -rf /var/lib/apt/lists/* \
 && ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /opt/java

COPY download-jars.sh /tmp/download-jars.sh
RUN sh /tmp/download-jars.sh /opt/spark-jars

COPY requirements-pipeline.txt /tmp/requirements-pipeline.txt
RUN python -m venv /opt/scipulse-venv \
 && /opt/scipulse-venv/bin/pip install --no-cache-dir --upgrade pip \
 && /opt/scipulse-venv/bin/pip install --no-cache-dir -r /tmp/requirements-pipeline.txt \
 && chmod -R a+rX /opt/scipulse-venv /opt/spark-jars

USER airflow

ENV JAVA_HOME=/opt/java \
    SCIPULSE_PYTHON=/opt/scipulse-venv/bin/python \
    SCIPULSE_DBT=/opt/scipulse-venv/bin/dbt \
    SPARK_SUBMIT=/opt/scipulse-venv/bin/spark-submit \
    PYSPARK_DRIVER_PYTHON=/opt/scipulse-venv/bin/python \
    PYSPARK_PYTHON=python3 \
    SPARK_EXTRA_JARS=/opt/spark-jars/hadoop-aws-3.3.4.jar,/opt/spark-jars/aws-java-sdk-bundle-1.12.262.jar,/opt/spark-jars/elasticsearch-spark-30_2.12-8.13.4.jar
