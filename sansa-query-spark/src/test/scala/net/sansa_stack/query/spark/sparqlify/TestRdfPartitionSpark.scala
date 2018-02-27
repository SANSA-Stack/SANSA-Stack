package net.sansa_stack.query.spark.sparqlify

import java.io.ByteArrayInputStream

import scala.collection.JavaConverters._
import scala.reflect.runtime.universe._

import org.aksw.jena_sparql_api.core.FluentQueryExecutionFactory
import org.apache.jena.query.Query
import org.apache.jena.graph.Node
import org.apache.spark.sql.SparkSession
import org.scalatest._

import benchmark.generator.Generator
import benchmark.serializer.SerializerModel
import net.sansa_stack.rdf.spark.partition.core.RdfPartitionUtilsSpark
import org.aksw.jena_sparql_api.stmt.SparqlQueryParserImpl
import org.apache.jena.query.ResultSetFormatter
import org.apache.jena.riot.RDFDataMgr
import org.apache.jena.graph.Triple
import org.apache.jena.riot.Lang


class TestRdfPartitionSpark extends FlatSpec {

  "A partitioner" should "support custom datatypes" in {

    val serializer = new SerializerModel()
    Generator.init(Array[String]())
    Generator.setSerializer(serializer)
    Generator.run()
    val testDriverParams = Generator.getTestDriverParams

    val model = serializer.getModel

    val sparkSession = SparkSession.builder
      .master("local[*]")
      .config("spark.sql.crossJoin.enabled", "true")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrator", String.join(", ",
        "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator",
        "net.sansa_stack.query.spark.sparqlify.KryoRegistratorSparqlify"))
      .appName("Partitioner test")
      .getOrCreate()

    val triples = model.getGraph.find(Node.ANY, Node.ANY, Node.ANY).toList.asScala
    val graphRdd = sparkSession.sparkContext.parallelize(triples)


    val partitions = RdfPartitionUtilsSpark.partitionGraph(graphRdd)
    val rewriter = SparqlifyUtils3.createSparqlSqlRewriter(sparkSession, partitions)

    val qef = new QueryExecutionFactorySparqlifySpark(sparkSession, rewriter)

    val str = """
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        SELECT * {
          ?s
            rdfs:label ?l ;
            rdfs:comment ?c
        }
    """

    println(ResultSetFormatter.asText(qef.createQueryExecution(str).execSelect()))

    sparkSession.stop

    // TODO Validate result - right now its already a success if no exception is thrown
  }
}

