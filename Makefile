default:
	mvn clean package

run:
	spark-submit --class net.sansa_stack.rdf.spark.App --master spark://localhost:7077 target/sansa-rdf-spark-0.0.1-SNAPSHOT-jar-with-dependencies.jar /tmp/input /tmp/output

test:
	mvn test
