FROM bde2020/spark-java-template:2.0.1-hadoop2.7

MAINTAINER Gezim Sejdiu <g.sejdiu@gmail.com>

ENV SPARK_APPLICATION_JAR_NAME sansa-examples-spark-1.1-with-dependencies
ENV SPARK_APPLICATION_MAIN_CLASS net.sansa_stack.examples.spark.rdf.TripleReader
ENV SPARK_APPLICATION_ARGS "hdfs://namenode:8020/user/hue/input/rdf.nt hdfs://namenode:8020/user/hue/output/result.nt"

ENV HDFS_URL=hdfs://hdfs:9000
