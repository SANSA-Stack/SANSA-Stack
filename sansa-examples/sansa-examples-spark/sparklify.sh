/opt/spark/current/bin/spark-submit --class net.sansa_stack.examples.spark.query.Sparklify \
--conf "spark.cores.max=2" \
--conf "spark.driver.memory=1G" \
--conf "spark.executor.memory=1G" \
--master spark://ravenxps:7077 \
"/home/raven/Projects/Eclipse/sansa-parent/sansa-examples-parent/sansa-examples-spark/target/sansa-examples-spark_2.12-0.7.2-SNAPSHOT-jar-with-dependencies.jar" \
-i "file:///home/raven/Projects/Eclipse/embeddable-bsbm-parent/bsbm-core/dataset.nt" \
-r "endpoint"

