#!/bin/sh
# Jars Maven nécessaires au job Spark (versions alignées sur PySpark 3.5.3 → Hadoop 3.3.4)
#   hadoop-aws + aws-java-sdk-bundle : lecture s3a:// (MinIO)
#   elasticsearch-spark-30_2.12     : écriture Gold vers Elasticsearch 8.13
set -e
DEST="${1:-/opt/spark-jars}"
mkdir -p "$DEST"
MAVEN=https://repo1.maven.org/maven2
for path in \
  org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
  com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
  org/elasticsearch/elasticsearch-spark-30_2.12/8.13.4/elasticsearch-spark-30_2.12-8.13.4.jar
do
  curl -fsSL -o "$DEST/$(basename "$path")" "$MAVEN/$path"
done
ls -l "$DEST"
