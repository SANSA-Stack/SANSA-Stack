FROM bde2020/spark-java-template:1.6.2-hadoop2.6

MAINTAINER Gezim Sejdiu <g.sejdiu@gmail.com>

ENV SPARK_APPLICATION_MAIN_CLASS net.sansa_stack.rdf.spark.App
ENV SPARK_APPLICATION_JAR_NAME sansa-rdf-spark-0.0.1-SNAPSHOT-jar-with-dependencies
ENV SPARK_APPLICATION_ARGS "hdfs://namenode:8020/user/root/input/ hdfs://namenode:8020/user/root/output/"
ENV HDFS_URL=hdfs://hdfs:9000
