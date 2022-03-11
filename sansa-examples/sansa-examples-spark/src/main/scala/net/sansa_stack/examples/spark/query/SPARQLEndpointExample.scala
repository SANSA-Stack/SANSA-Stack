package net.sansa_stack.examples.spark.query

import com.typesafe.scalalogging.Logger
import net.sansa_stack.query.spark.SPARQLEngine
import net.sansa_stack.query.spark.SPARQLEngine.{Ontop, SPARQLEngine, Sparqlify}
import net.sansa_stack.query.spark.api.impl.QueryEngineFactoryBase
import net.sansa_stack.query.spark.ontop.QueryEngineFactoryOntop
import net.sansa_stack.query.spark.sparqlify.QueryEngineFactorySparqlify
import net.sansa_stack.rdf.spark.io._
import org.aksw.jenax.server.utils.FactoryBeanSparqlServer
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException
import org.apache.jena.query.{QueryFactory, ResultSetFormatter}
import org.apache.jena.riot.{Lang, RDFDataMgr, RDFLanguages}
import org.apache.jena.sys.JenaSystem
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.DatabaseAlreadyExistsException

import java.net.URI

/**
 * This example shows how to run SPARQL queries over Spark using a SPARQL-to-SQL rewriter under the hood.
 * The RDF data can be provided either as triples file or even as a pre-partitioned dataset maintained in tables
 * of a Spark database and a corresponding R2RML mapping file.
 *
 * Two different modes for demonstrator purposes can be used:
 * - run a single query
 * - start a SPARQL endpoint with a Web UI as optional interface
 *
 */
object SPARQLEndpointExample {

  JenaSystem.init()

  def main(args: Array[String]) {
    parser.parse(args, Config()) match {
      case Some(config) =>
        run(config.in, config.database, config.mappingsfile, config.queryEngine,
          config.sparql, config.runMode, config.port)
      case None =>
        println(parser.usage)
    }
  }

  def run(input: String,
          database: String,
          mappingsFile: URI,
          queryEngine: SPARQLEngine.Value,
          sparqlQuery: String = "",
          mode: Option[String],
          port: Int = 7531): Unit = {

    println("======================================")
    println("|   SPARQL example                   |")
    println("======================================")

    val spark = SparkSession.builder
      .appName(s"SPARQL example ( $input )")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrator", String.join(
        ", ",
        "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator",
        "net.sansa_stack.query.spark.ontop.OntopKryoRegistrator"))
      .config("spark.sql.crossJoin.enabled", true)
      .enableHiveSupport()
      .getOrCreate()

    // we build the query engine here
    val queryEngineFactory: QueryEngineFactoryBase = queryEngine match {
      case Ontop => new QueryEngineFactoryOntop(spark)
      case Sparqlify => new QueryEngineFactorySparqlify(spark)
      case _ => throw new RuntimeException("Unsupported query engine")
    }

    // load the data into an RDD
    if (database != null) { // pre-partitioned case
      // we do catch the exception here which is always thrown because of ...
      scala.util.control.Exception.catching(classOf[DatabaseAlreadyExistsException], classOf[AlreadyExistsException])(spark.sql("CREATE DATABASE IF NOT EXISTS " + database))
      spark.sql("USE " + database)
    }

    // create the query execution factory
    val qef =
      if (mappingsFile != null) {
        // load the R2RML mappings
        val mappings = RDFDataMgr.loadModel(mappingsFile.toString)
        queryEngineFactory.create(Option(database), mappings)
      } else {
        // triples.verticalPartition(RdfPartitionerDefault).r2rmlModel
        val lang = RDFLanguages.filenameToLang(input)
//        val lang = Lang.NTRIPLES
        val triples = spark.rdf(lang)(input)
        import net.sansa_stack.rdf.spark.partition._

        // TODO Maybe support getting the partitioner from the queryEngineFactory
        //  such as queryEngineFactory.newPartitioner.apply(triples)
        val partitioner = queryEngineFactory.getPartitioner
        val mappings = triples.verticalPartition(partitioner).r2rmlModel

        queryEngineFactory.create(Option(database), mappings)
      }

    // run i) a single SPARQL query and terminate or ii) host some SNORQL web UI
    mode.get match {
      case "batch" =>
        // only SELECT queries will be considered here
        val query = QueryFactory.create(sparqlQuery)
        val qe = qef.createQueryExecution(query)

        val rs = qe.execSelect()
        // show bindings on command line
        ResultSetFormatter.out(rs)
      case "endpoint" =>
        val server = FactoryBeanSparqlServer.newInstance.setSparqlServiceFactory(qef).setPort(port).create()
//        if (Desktop.isDesktopSupported) {
//          Desktop.getDesktop.browse(URI.create("http://localhost:" + port + "/sparql"))
//        }
        server.join()
      case _ => // should never happen
    }

    spark.stop
  }

  implicit val sparqlEngineRead: scopt.Read[SPARQLEngine.Value] = scopt.Read.reads(SPARQLEngine.withName)

  case class Config(in: String = null,
                    database: String = null,
                    mappingsfile: URI = null,
                    queryEngine: SPARQLEngine.Value = SPARQLEngine.Ontop,
                    sparql: String = "SELECT * WHERE {?s ?p ?o} LIMIT 10",
                    runMode: Option[String] = None,
                    port: Int = 7531,
                    browser: Boolean = true)

  val parser = new scopt.OptionParser[Config]("SPARQLEngineExample") {

    head("SPARQL example")

    opt[String]('i', "input")
      .valueName("<path>").
      action((x, c) => c.copy(in = x)).
      text("path to file that contains the data (in N-Triples format)")

    opt[String]("database")
      .abbr("db")
      .action((x, c) => c.copy(database = x))
      .text("the name of the Spark database used as KB")

    opt[URI]( "mappings-file")
      .action((x, c) => c.copy(mappingsfile = x))
      .text("path to R2RML mappings")

    opt[SPARQLEngine]("sparql-engine")
      .action((x, c) => c.copy(queryEngine = x))
      .optional()
      .text("the SPARQL backend ('Ontop', 'Sparqlify')")

    checkConfig(c =>
      if (c.runMode.isDefined) success else failure("A command is required.")
    )


    help("help").text("prints this usage text")

    cmd("endpoint")
      .action((_, c) => c.copy(runMode = Some("endpoint")))
      .text("start a SPARQL endpoint")
      .children(
        opt[Int]('p', "port").optional().valueName("<port-number>").
          action((x, c) => c.copy(port = x)).
          text("port that SPARQL endpoint will be exposed, default:'7531'")
      )

    cmd("batch")
      .action((_, c) => c.copy(runMode = Some("batch")))
      .text("run a single SPARQL query and show results")
      .children(
        opt[String]('q', "query").valueName("<query>").
          action((x, c) => c.copy(sparql = x)).
          validate(x => try {
            val q = QueryFactory.create(x)
            if (q.isSelectType) success else failure("Only SELECT queries are supported in the demo")
          } catch {
            case e: Exception =>
              e.printStackTrace()
              failure("Must be a valid SPARQL query.")
          }).
          text("a SPARQL query")
    )

  }

}
