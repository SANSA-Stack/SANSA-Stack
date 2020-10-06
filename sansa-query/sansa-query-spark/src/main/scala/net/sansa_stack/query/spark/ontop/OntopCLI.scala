package net.sansa_stack.query.spark.ontop

import java.io.File
import java.net.URI
import java.nio.file.Paths

import scala.collection.JavaConverters._

import org.apache.jena.query.{QueryFactory, QueryType}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.model.{HasDataPropertiesInSignature, HasObjectPropertiesInSignature, OWLOntology}
import scopt.OParser

import net.sansa_stack.rdf.common.partition.core.{RdfPartitionComplex, RdfPartitionerComplex}
import net.sansa_stack.rdf.spark.partition.core.RdfPartitionUtilsSpark

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

    val sparqlEngine =
      if (config.inputPath != null) {
        // read triples as RDD[Triple]
        var triplesRDD = spark.ntriples()(config.inputPath.toString).cache()

        // load optional schema file and filter properties used for VP
        var ont: OWLOntology = null

        if (config.schemaPath != null) {
          val owlFile = new File(config.schemaPath)
          val man = OWLManager.createOWLOntologyManager()
          ont = man.loadOntologyFromOntologyDocument(owlFile)
          //    val cleanOnt = man.createOntology()
          //    man.addAxioms(cleanOnt, ont.asInstanceOf[HasLogicalAxioms].getLogicalAxioms)
          //
          //    owlFile = "/tmp/clean-dbo.nt"
          //    man.saveOntology(cleanOnt, new FileOutputStream(owlFile))

//          // get all object properties in schema file
//          val objectProperties = ont.asInstanceOf[HasObjectPropertiesInSignature].getObjectPropertiesInSignature.iterator().asScala.map(_.toStringID).toSet
//
//          // get all object properties in schema file
//          val dataProperties = ont.asInstanceOf[HasDataPropertiesInSignature].getDataPropertiesInSignature.iterator().asScala.map(_.toStringID).toSet
//
//          var schemaProperties = objectProperties ++ dataProperties
//          schemaProperties = Set("http://dbpedia.org/ontology/birthPlace", "http://dbpedia.org/ontology/birthDate")
//
//          // filter triples RDD
//          triplesRDD = triplesRDD.filter(t => schemaProperties.contains(t.getPredicate.getURI))
        }

        // do partitioning here
        logger.info("computing partitions ...")
        val partitions: Map[RdfPartitionComplex, RDD[Row]] = RdfPartitionUtilsSpark.partitionGraph(triplesRDD, partitioner = RdfPartitionerComplex(false))
        logger.info(s"got ${partitions.keySet.size} partitions ...")

        // create the SPARQL engine
        OntopSPARQLEngine(spark, partitions, Option(ont))
      } else {
        // load partitioning metadata
        val partitions = PartitionSerDe.deserializeFrom(Paths.get(config.metaDataPath))
        // create the SPARQL engine
        OntopSPARQLEngine(spark, config.databaseName, partitions, None)
      }

    var input = config.initialQuery

    def runQuery(query: String): Unit = {
      try {
        val q = QueryFactory.create(query)

        q.queryType() match {
          case QueryType.SELECT =>
            val res = sparqlEngine.execSelect(query)
            println(res.collect().mkString("\n"))
          case QueryType.ASK =>
            val res = sparqlEngine.execAsk(query)
            println(res)
          case QueryType.CONSTRUCT =>
            val res = sparqlEngine.execConstruct(query)
            println(res.collect().mkString("\n"))
          case _ => throw new RuntimeException(s"unsupported query type: ${q.queryType()}")
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

    sparqlEngine.stop()

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
