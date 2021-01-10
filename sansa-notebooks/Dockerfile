FROM apache/zeppelin:0.9.0

LABEL maintainer="Farshad Bakhshandegan Moghaddam <f.bakhshandegan@gmail.com>"

ENV SPARK_VERSION=3.0.1
ENV HADOOP_VERSION=2.7

RUN wget https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
      && tar -xvzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
      && mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} spark \
      && rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \      
      && cd /
