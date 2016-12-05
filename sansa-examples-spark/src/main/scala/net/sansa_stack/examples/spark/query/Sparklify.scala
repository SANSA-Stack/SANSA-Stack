package net.sansa_stack.examples.spark.query

import java.io.File
import scala.collection.mutable
import org.apache.spark.sql.SparkSession
import net.sansa_stack.rdf.spark.graph.LoadGraph
import net.sansa_stack.rdf.spark.model.JenaSparkRDDOps
import net.sansa_stack.rdf.spark.partition.core.RdfPartitionUtilsSpark
import net.sansa_stack.rdf.partition.sparqlify.SparqlifyUtils2
import net.sansa_stack.rdf.spark.sparqlify.QueryExecutionFactorySparqlifySpark
import org.aksw.jena_sparql_api.server.utils.FactoryBeanSparqlServer
import net.sansa_stack.query.spark.server.SparqlifyUtils3
import org.apache.jena.riot.RDFDataMgr
import org.apache.jena.riot.Lang
import org.apache.commons.io.IOUtils

/*
 * Run SPARQL queries over Spark using Sparqlify approach.
 */
object Sparklify {

  def main(args: Array[String]) = {
    if (args.length < 1) {
      System.err.println(
        "Usage: Sparklify <input> ")
      System.exit(1)
    }
    val input =args(0)
    val optionsList = args.drop(1).map { arg =>
      arg.dropWhile(_ == '-').split('=') match {
        case Array(opt, v) => (opt -> v)
        case _             => throw new IllegalArgumentException("Invalid argument: " + arg)
      }
    }
    val options = mutable.Map(optionsList: _*)

    options.foreach {
      case (opt, _) => throw new IllegalArgumentException("Invalid option: " + opt)
    }
    println("======================================")
    println("|   Sparklify example                |")
    println("======================================")

    val sparkSession = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrator", String.join(", ",
        "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator",
        "net.sansa_stack.rdf.spark.sparqlify.KryoRegistratorSparqlify"))
      .appName("Sparklify example (" + input + ")")
      .getOrCreate()

    val ops = JenaSparkRDDOps(sparkSession.sparkContext)
    import ops._

    val it = sparkSession.sparkContext.textFile(input).collect.mkString("\n")

    val triples = fromNTriples(it, "http://dbpedia.org").toSeq
    val graphRdd = sparkSession.sparkContext.parallelize(triples)

    val partitions = RdfPartitionUtilsSpark.partitionGraph(graphRdd)
    val rewriter = SparqlifyUtils3.createSparqlSqlRewriter(sparkSession, partitions)
    val qef = new QueryExecutionFactorySparqlifySpark(sparkSession, rewriter)
    val server = FactoryBeanSparqlServer.newInstance.setSparqlServiceFactory(qef).create
    server.join()
    sparkSession.stop

  }

}