# Base: official Spark image with Python support.
FROM apache/spark:3.5.3-python3

USER root

# Delta Lake + S3A connector jars, matched to Spark 3.5 / Hadoop 3.
# These let Spark speak Delta and talk to S3/MinIO.
ARG DELTA_VERSION=3.2.1
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.262

RUN cd /opt/spark/jars && \
    curl -sLO https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/${DELTA_VERSION}/delta-spark_2.12-${DELTA_VERSION}.jar && \
    curl -sLO https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar && \
    curl -sLO https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar && \
    curl -sLO https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar

# copy our pipeline script into the image
COPY spark-jobs/pipeline_k8s.py /opt/spark/work-dir/pipeline_k8s.py

USER spark
