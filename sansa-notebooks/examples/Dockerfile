FROM bde2020/spark-submit:2.1.0-hadoop2.8-hive-java8

COPY jars /jars
COPY data /data

RUN chmod +x /wait-for-step.sh && chmod +x /execute-step.sh && chmod +x /finish-step.sh

ENV SPARK_MASTER_NAME spark-master
ENV SPARK_MASTER_PORT 7077
ENV SPARK_APPLICATION_JAR_LOCATION /jars/sansa-examples-spark.jar
ENV SPARK_APPLICATION_MAIN_CLASS net.sansa_stack.examples.spark.rdf.TripleReader
ENV SPARK_APPLICATION_ARGS ""
