default:
	mvn clean package

run:
	spark-submit --class org.dissect.rdf.spark.App --master spark://localhost:7077 target/dissect-rdf-spark-0.0.1-SNAPSHOT-jar-with-dependencies.jar Cocktails /tmp/input /tmp/output

test:
	mvn test
