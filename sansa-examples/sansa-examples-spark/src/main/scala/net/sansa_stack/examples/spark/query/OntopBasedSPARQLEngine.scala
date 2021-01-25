package net.sansa_stack.examples.spark.query

import java.awt.Desktop
import java.net.URI

import scala.collection.convert.ImplicitConversions.`iterator asScala`

import org.aksw.jena_sparql_api.server.utils.FactoryBeanSparqlServer
import org.aksw.sparqlify.core.sql.common.serialization.SqlEscaperDoubleQuote
import org.apache.jena.query.QueryFactory
import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.riot.Lang
import org.apache.jena.sys.JenaSystem
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

import net.sansa_stack.query.spark.ontop.{QueryEngineOntop, QueryExecutionFactorySparkOntop}
import net.sansa_stack.rdf.common.partition.core.{RdfPartitionStateDefault, RdfPartitionerComplex}
import net.sansa_stack.rdf.common.partition.r2rml.R2rmlUtils
import net.sansa_stack.rdf.spark.io._
import net.sansa_stack.rdf.spark.partition.core.{RdfPartitionUtilsSpark, SQLUtils, SparkTableGenerator}

/**
  * Run SPARQL queries over Spark using Ontop as SPARQL-to-SQL rewriter.
  */
object OntopBasedSPARQLEngine {

  JenaSystem.init

  def main(args: Array[String]) {
    parser.parse(args, Config()) match {
      case Some(config) =>
        run(config.in, config.database, config.partitioningMetadataPath, config.sparql, config.runMode, config.port)
      case None =>
        println(parser.usage)
    }
  }

  def run(input: String,
          database: String,
          partitioningMetadataPath: URI,
          sparqlQuery: String = "",
          run: String = "cli",
          port: Int = 7531): Unit = {

    println("======================================")
    println("|   Ontop based SPARQL example       |")
    println("======================================")

    val spark = SparkSession.builder
      .appName(s"Ontop SPARQL example ( $input )")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrator", String.join(
        ", ",
        "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator",
        "net.sansa_stack.query.spark.ontop.OntopKryoRegistrator"))
      .config("spark.sql.crossJoin.enabled", true)
      .enableHiveSupport()
      .getOrCreate()

    // load the data into an RDD
    val lang = Lang.NTRIPLES
    val triples = spark.rdf(lang)(input)

    // apply vertical partitioning which is necessary for the current Ontop integration
    val partitioner = RdfPartitionerComplex()
    val partitions2RDD: Map[RdfPartitionStateDefault, RDD[Row]] = RdfPartitionUtilsSpark.partitionGraph(triples, partitioner)

    val mappingsModel = ModelFactory.createDefaultModel()
    val partitions = partitions2RDD.keySet.toSeq

    val tableNameFn: RdfPartitionStateDefault => String = p => SQLUtils.escapeTablename(R2rmlUtils.createDefaultTableName(p))
    SparkTableGenerator(spark).createAndRegisterSparkTables(partitioner,
                                                            partitions2RDD,
                                                            extractTableName = tableNameFn)
    R2rmlUtils.createR2rmlMappings(partitioner, partitions, tableNameFn, new SqlEscaperDoubleQuote(), mappingsModel, true, escapeIdentifiers = true)

    val ontop = QueryEngineOntop(spark, "test", mappingsModel, None)

    val qef = new QueryExecutionFactorySparkOntop(spark, ontop)

    // run i) a single SPARQL query and terminate or ii) host some SNORQL web UI
    run match {
      case "cli" =>
        // only SELECT queries will be considered here
        val query = QueryFactory.create(sparqlQuery)
        val qe = qef.createQueryExecution(query)

        val result = qe.execSelect()
        // show bindings on command line
        result.foreach(println)
      case "endpoint" =>
        val server = FactoryBeanSparqlServer.newInstance.setSparqlServiceFactory(qef).setPort(port).create()
        if (Desktop.isDesktopSupported) {
          Desktop.getDesktop.browse(URI.create("http://localhost:" + port + "/sparql"))
        }
        server.join()
      case _ => // should never happen
    }

    spark.stop

  }

  case class Config(in: String = null,
                    database: String = null,
                    partitioningMetadataPath: URI = null,
                    sparql: String = "SELECT * WHERE {?s ?p ?o} LIMIT 10",
                    runMode: String = "cli",
                    port: Int = 7531,
                    browser: Boolean = true)

  val parser = new scopt.OptionParser[Config]("Ontop SPARQL example") {

    head("Ontop SPARQL example")

    opt[String]('i', "input")
      .valueName("<path>").
      action((x, c) => c.copy(in = x)).
      text("path to file that contains the data (in N-Triples format)")

    opt[URI]( "metadata")
      .action((x, c) => c.copy(partitioningMetadataPath = x))
      .text("path to partitioning metadata")

    opt[String]("database")
      .abbr("db")
      .action((x, c) => c.copy(database = x))
      .text("the name of the Spark database used as KB")

    opt[String]('q', "query").optional().valueName("<query>").
      action((x, c) => c.copy(sparql = x)).
      validate(x => try {
        QueryFactory.create(x)
        success
      } catch {
        case e: Exception =>
          e.printStackTrace()
          failure("Must be a valid SPARQL query.")
      }).
      text("a SPARQL query")

    opt[String]('m', "mode").optional().valueName("run mode").
      action((x, c) => c.copy(runMode = x)).
      validate(x => if (x == "cli" || x == "endpoint") success
                    else failure("wrong run mode: use either 'cli' or 'endpoint'")).
      text("Runner mode ('cli', 'endpoint'). Default:'cli'")

    opt[Int]('p', "port").optional().valueName("port").
      action((x, c) => c.copy(port = x)).
      text("port that SPARQL endpoint will be exposed, default:'7531'")

    checkConfig(c =>
      if (c.runMode == "cli" && c.sparql.isEmpty) failure("Option --sparql must not be empty if cli is enabled")
      else success)

    help("help").text("prints this usage text")
  }

}
