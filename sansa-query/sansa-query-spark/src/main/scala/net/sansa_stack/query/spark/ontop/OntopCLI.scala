package net.sansa_stack.query.spark.ontop

import java.net.URI

import org.apache.jena.query.{QueryFactory, QueryType, ResultSetFormatter}
import org.apache.jena.riot.RDFDataMgr
import org.apache.spark.sql.SparkSession
import scopt.OParser

/**
 * @author Lorenz Buehmann
 */
class OntopCLI {

  val logger = com.typesafe.scalalogging.Logger(classOf[OntopCLI])

  def run(args: Array[String]): Unit = {
    OParser.parse(parser, args, Config()) match {
      case Some(config) =>
        run(config)
      case _ =>
      // arguments are bad, error message will have been displayed
    }
  }

  private def run(config: Config): Unit = {
    import net.sansa_stack.rdf.spark.io._

    val spark = SparkSession.builder
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // .config("spark.kryo.registrationRequired", "true")
      // .config("spark.eventLog.enabled", "true")
      .config("spark.kryo.registrator", String.join(", ",
        "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator",
        "net.sansa_stack.query.spark.ontop.OntopKryoRegistrator"))
      .config("spark.sql.cbo.enabled", true)
      .config("spark.sql.statistics.histogram.enabled", true)
      .config("spark.sql.crossJoin.enabled", "true")
      .enableHiveSupport()
      .getOrCreate()

    val qef =
      if (config.inputPath != null) {
        // read triples as RDD[Triple]
        val triplesRDD = spark.ntriples()(config.inputPath.toString).cache()

        // create the SPARQL engine
        import net.sansa_stack.query.spark._
        triplesRDD.sparql(SPARQLEngine.Ontop)
      } else {
        val mappingsModel = RDFDataMgr.loadModel(config.metaDataPath.toString)
        new QueryEngineFactoryOntop(spark).create(config.databaseName, mappingsModel)
      }

    var input = config.initialQuery

    def runQuery(query: String): Unit = {
      try {
        val q = QueryFactory.create(query)

        val qe = qef.createQueryExecution(q)

        val qt = q.getClass.getDeclaredMethod("queryType").invoke(q).asInstanceOf[QueryType] // TODO Scala compiler from CLI fails without reflection ...

        qt match {
          case QueryType.SELECT =>
            val rs = qe.execSelect()
            ResultSetFormatter.out(rs)
          case QueryType.ASK =>
            val res = qe.execAsk()
            println(res)
          case QueryType.CONSTRUCT =>
            val res = qe.execConstruct()
            res.write(System.out, "Turtle")
          case _ => throw new RuntimeException(s"unsupported query type: ${qt.name()}")
        }
      } catch {
        case e: Exception => Console.err.println("failed to execute query")
          e.printStackTrace()
      }
    }

    if (input != null) {
      runQuery(input)
    }
    println("enter SPARQL query (press 'q' to quit): ")
    input = scala.io.StdIn.readLine()
    while (input != "q") {
      runQuery(input)
      println("enter SPARQL query (press 'q' to quit): ")
      input = scala.io.StdIn.readLine()
    }

    spark.stop()
  }

  case class Config(
                     inputPath: URI = null,
                     metaDataPath: URI = null,
                     schemaPath: URI = null,
                     databaseName: String = null,
                     initialQuery: String = null)

  import scopt.OParser
  val builder = OParser.builder[Config]
  val parser = {
    import builder._
    OParser.sequence(
      programName("Ontop SPARQL Engine"),
      head("Ontop SPARQL Engine", "0.1.0"),
      opt[URI]('i', "input")
        .action((x, c) => c.copy(inputPath = x))
        .text("path to input data"),
      opt[URI]('m', "metadata")
        .action((x, c) => c.copy(metaDataPath = x))
        .text("path to partitioning metadata"),
      opt[String]("database")
        .abbr("db")
        .action((x, c) => c.copy(databaseName = x))
        .text("the name of the Spark databases used as KB"),
      opt[URI]('s', "schema")
        .optional()
        .action((x, c) => c.copy(schemaPath = x))
        .text("an optional file containing the OWL schema to process only object and data properties"),
      opt[String]('q', "query")
        .optional()
        .action((x, c) => c.copy(initialQuery = x))
        .text("an initial SPARQL query that will be executed"),
      checkConfig( c =>
        if (c.databaseName != null && c.inputPath != null) failure("either specify path to data or an already created database")
        else success ),
      checkConfig( c =>
        if (c.databaseName != null && c.metaDataPath == null) failure("If database is used the path to the partitioning " +
          "metadata has to be provided as well")
        else success )
    )
  }

}
