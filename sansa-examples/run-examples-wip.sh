LAYER=query
EXAMPLE=Sparklify

JAR=`ls sansa-examples-spark/target/sansa-examples-spark_*-dist.jar`
BASE_URL="file://"`pwd`"/"
echo "Using jar file $JAR"
echo "Base URL: $BASE_URL"

spark-submit \
  	--class net.sansa_stack.examples.spark.$LAYER.$EXAMPLE \
  	--master spark://spark-master:7077 \
  	"$JAR" -i "$BASE_URL/sansa-examples-spark/src/main/resources/rdf.nt" \

# TODO Validate the output
curl -LH 'Accept: application/sparql-results+json' 'http://localhost:7531/sparql?query=SELECT%20%2A%20%7B%20%3Fs%20%3Fp%20%3Fo%20%7D'

