package net.sansa_stack.query.spark.sparqlify

import java.io.ByteArrayInputStream

import scala.collection.JavaConverters._
import scala.reflect.runtime.universe._

import org.aksw.jena_sparql_api.core.FluentQueryExecutionFactory
import org.apache.jena.query.Query
import org.apache.jena.graph.Node
import org.apache.spark.sql.SparkSession
import org.scalatest._
import com.holdenkarau.spark.testing.DataFrameSuiteBase

import benchmark.generator.Generator
import benchmark.serializer.SerializerModel
import net.sansa_stack.rdf.spark.partition.core.RdfPartitionUtilsSpark
import org.aksw.jena_sparql_api.stmt.SparqlQueryParserImpl
import org.apache.jena.query.ResultSetFormatter
import org.apache.jena.riot.RDFDataMgr
import org.apache.jena.graph.Triple
import org.apache.jena.riot.Lang

class TestRdfPartitionSpark extends FunSuite with DataFrameSuiteBase {

  test("A partitioner should support custom datatypes"){

    val serializer = new SerializerModel()
    Generator.init(Array[String]())
    Generator.setSerializer(serializer)
    Generator.run()
    val testDriverParams = Generator.getTestDriverParams

    val model = serializer.getModel

    val triples = model.getGraph.find(Node.ANY, Node.ANY, Node.ANY).toList.asScala
    val graphRdd = spark.sparkContext.parallelize(triples)

    val partitions = RdfPartitionUtilsSpark.partitionGraph(graphRdd)
    val rewriter = SparqlifyUtils3.createSparqlSqlRewriter(spark, partitions)

    val qef = new QueryExecutionFactorySparqlifySpark(spark, rewriter)

    val str = """
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        SELECT * {
          ?s
            rdfs:label ?l ;
            rdfs:comment ?c
        }
    """

    println(ResultSetFormatter.asText(qef.createQueryExecution(str).execSelect()))

    // TODO Validate result - right now its already a success if no exception is thrown
  }
}

