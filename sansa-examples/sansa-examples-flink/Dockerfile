FROM bde2020/flink-maven-template:1.1.3-hadoop2.7

MAINTAINER Gezim Sejdiu <g.sejdiu@gmail.com>

ENV FLINK_APPLICATION_JAR_NAME sansa-examples-flink-1.1-with-dependencies
ENV FLINK_APPLICATION_MAIN_CLASS net.sansa_stack.examples.flink.rdf.TripleReader
ENV FLINK_APPLICATION_ARGS "hdfs://namenode:8020/user/root/input/rdf.nt"
