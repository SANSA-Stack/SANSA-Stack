package net.sansa_stack.inference.spark.data.loader

import java.net.URI

import net.sansa_stack.inference.data.{SQLSchema, SQLSchemaDefault}
import net.sansa_stack.inference.spark.data.model.{RDFGraph, RDFGraphDataFrame, RDFGraphDataset, RDFGraphNative}
import net.sansa_stack.inference.utils.NTriplesStringToJenaTriple
import org.apache.jena.graph.Triple
import org.apache.jena.riot.Lang
import org.apache.spark.sql.{Dataset, Encoder, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

import scala.language.implicitConversions
import org.apache.jena.vocabulary.RDF

/**
  * A class that provides methods to load an RDF graph from disk.
  *
  * @author Lorenz Buehmann
  *
  */
object RDFGraphLoader {

  private val logger = com.typesafe.scalalogging.Logger(LoggerFactory.getLogger(this.getClass.getName))

  private implicit def pathURIsConverter(uris: Seq[URI]): String = uris.map(p => p.toString).mkString(",")

  /**
    * Load an RDF graph from a file or directory. The path can also contain multiple paths
    * and even wildcards, e.g.
    * `"/my/dir1,/my/paths/part-00[0-5]*,/another/dir,/a/specific/file"`
    *
    * @param path the absolute path of the file
    * @param session the Spark session
    * @param minPartitions min number of partitions for Hadoop RDDs ([[SparkContext.defaultMinPartitions]])
    * @return an RDF graph
    */
  def loadFromDisk(session: SparkSession, path: String, minPartitions: Int = 2): RDFGraph = {
    logger.info("loading triples from disk...")
    val startTime = System.currentTimeMillis()

    val triples = session.sparkContext
      .textFile(path, minPartitions) // read the text file
      .filter(line => !line.trim().isEmpty & !line.startsWith("#"))
      .map(new NTriplesStringToJenaTriple()) // convert to triple object
//      .repartition(minPartitions)

//  logger.info("finished loading " + triples.count() + " triples in " + (System.currentTimeMillis()-startTime) + "ms.")
    RDFGraph(triples)
  }

  /**
    * Load an RDF graph from multiple files or directories.
    *
    * @param paths the files or directories
    * @param session the Spark session
    * @param minPartitions min number of partitions for Hadoop RDDs ([[SparkContext.defaultMinPartitions]])
    * @return an RDF graph
    */
  def loadFromDisk(session: SparkSession, paths: Seq[URI], minPartitions: Int): RDFGraph = {
    loadFromDisk(session, paths.mkString(","), minPartitions)
  }

  /**
    * Load an RDF graph from a single file or directory.
    *
    * @param path the path to the file or directory
    * @param session the Spark session
    * @param minPartitions min number of partitions for Hadoop RDDs ([[SparkContext.defaultMinPartitions]])
    * @return an RDF graph
    */
  def loadFromDisk(session: SparkSession, path: URI, minPartitions: Int): RDFGraph = {
    loadFromDisk(session, Seq(path), minPartitions)
  }

  /**
    * Load an RDF graph from a file or directory with a Spark RDD as underlying datastructure.
    * The path can also contain multiple paths and even wildcards, e.g.
    * "/my/dir1,/my/paths/part-00[0-5]*,/another/dir,/a/specific/file"
    *
    * @param path the files
    * @param session the Spark session
    * @param minPartitions min number of partitions for Hadoop RDDs ([[SparkContext.defaultMinPartitions]])
    * @return an RDF graph
    */
  def loadFromDiskAsRDD(session: SparkSession, path: String, minPartitions: Int): RDFGraphNative = {
    logger.info("loading triples from disk...")
    val startTime = System.currentTimeMillis()

    val converter = new NTriplesStringToJenaTriple()

    val triples = session.sparkContext
      .textFile(path, minPartitions) // read the text file
      .map(line => converter.apply(line)) // convert to triple object

    // logger.info("finished loading " + triples.count() + " triples in " +
    // (System.currentTimeMillis()-startTime) + "ms.")
    new RDFGraphNative(triples)
  }

  private case class RDFTriple2(s: String, p: String, o: String) extends Product3[String, String, String] {
    override def _1: String = s
    override def _2: String = p
    override def _3: String = o

    def subject: String = s

    override def toString: String = s + "  " + p + "  " + o
  }

  /**
    * Load an RDF graph from from multiple files or directories with a Spark Dataset as underlying datastructure.
    * The path can also contain multiple paths and even wildcards, e.g.
    * `"/my/dir1,/my/paths/part-00[0-5]*,/another/dir,/a/specific/file"`
    *
    * @param path the absolute path of the file
    * @param session the Spark session
    * @return an RDF graph based on a [[Dataset]]
    */
  def loadFromDiskAsDataset(session: SparkSession, path: String): RDFGraphDataset = {
    logger.info("loading triples from disk...")
    val startTime = System.currentTimeMillis()

    import org.apache.spark.sql.functions._
    val splitter = udf((str: String) => {
      val splitted = str.split(" ").lift
      Array(splitted(0), splitted(1), splitted(2))
    })

    implicit val rdfTripleEncoder: Encoder[Triple] = org.apache.spark.sql.Encoders.kryo[Triple]
    val spark = session.sqlContext



    val triples = session.read
      .textFile(path) // read the text file
      .map(new NTriplesStringToJenaTriple())
      .as[Triple](rdfTripleEncoder)
      .as("triples")
    // (rdfTripleEncoder)
    //    val rowRDD = session.sparkContext
    //      .textFile(paths) // read the text file
    //      .map(s => {
    //      val tokens = s.split(" ")
    //      Row(tokens(0), tokens(1), tokens(2))
    //      //      RDFTriple(tokens(0), tokens(1), tokens(2))
    //    })
    //
    //    val encoder = Encoders.product[RDFTriple]
    //    val schema =
    //      StructType(Array(
    //        StructField("s", StringType, true),
    //        StructField("p", StringType, true),
    //        StructField("o", StringType, true)))
    //    val triplesDF = spark.createDataFrame(rowRDD, schema)
    //     val triples = triplesDF.as[RDFTriple](encoder)
    //    session.read
    //      .textFile(paths) // read the text file
    //      .map(s => {
    //      val tokens = s.split(" ")
    //      RDFTriple2(tokens(0), tokens(1), tokens(2))
    //    }).as[RDFTriple2].show(10)
    //      .select(splitter($"value") as "tokens")
    //      .select($"tokens"(0) as "s", $"tokens"(1) as "p", $"tokens"(2) as "o")
    //      .as[RDFTriple]


    // convert to triple object

    // logger.info("finished loading " + triples.count() + " triples in " +g
    // (System.currentTimeMillis()-startTime) + "ms.")
    new RDFGraphDataset(triples)
  }

  /**
    * Load an RDF graph from from from a file or directory with a Spark Dataset as underlying datastructure.
    * The path can also contain multiple paths and even wildcards, e.g.
    * `"/my/dir1,/my/paths/part-00[0-5]*,/another/dir,/a/specific/file"`
    *
    * @param paths the absolute path of the file
    * @param session the Spark session
    * @return an RDF graph based on a [[Dataset]]
    */
  def loadFromDiskAsDataset(session: SparkSession, paths: scala.Seq[URI]): RDFGraphDataset = {
    loadFromDiskAsDataset(session, paths.mkString(","))
  }

  /**
    * Load an RDF graph from a file or directory with a Spark DataFrame as underlying datastructure.
    * The path can also contain multiple paths and even wildcards, e.g.
    * `"/my/dir1,/my/paths/part-00[0-5]*,/another/dir,/a/specific/file"`
    *
    * @param path the absolute path of the file
    * @param session the Spark session
    * @param minPartitions min number of partitions for Hadoop RDDs ([[SparkContext.defaultMinPartitions]])
    * @return an RDF graph based on a [[org.apache.spark.sql.DataFrame]]
    */
  def loadFromDiskAsDataFrame(session: SparkSession, path: String, minPartitions: Int = 4, sqlSchema: SQLSchema = SQLSchemaDefault): RDFGraphDataFrame = {
    val df = session
      .read
      .format("net.sansa_stack.inference.spark.data.loader.sql")
      .load(path)

    // register the DataFrame as a table
    df.createOrReplaceTempView(sqlSchema.triplesTable)

    new RDFGraphDataFrame(df)
  }

  def main(args: Array[String]): Unit = {
    import net.sansa_stack.rdf.spark.io._

    val path = args(0)
    val lang = args(1) match {
      case "turtle" => Lang.TURTLE
      case "ntriples" => Lang.NTRIPLES
      case _ => null
    }

    val numThreads = if (args.length > 2)  args(2).toInt else 4
    val parallelism = if (args.length > 3)  args(3).toInt else 4

    val conf = new SparkConf()
    conf.registerKryoClasses(Array(classOf[org.apache.jena.graph.Triple]))
    conf.set("spark.extraListeners", "net.sansa_stack.inference.spark.utils.CustomSparkListener")
    conf.set("textinputformat.record.delimiter", ".\n")
    // the SPARK config
    val session = SparkSession.builder
      .appName(s"SPARK ${lang.getLabel} Loading")
      .master(s"local[$numThreads]")
      .config("spark.eventLog.enabled", "true")
      .config("spark.hadoop.validateOutputSpecs", "false") // override output files
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.default.parallelism", parallelism)
      .config("spark.ui.showConsoleProgress", "false")
      .config("spark.sql.shuffle.partitions", parallelism)
      .config(conf)
      .getOrCreate()


    val triples = session.read.rdf(lang)(path)
    triples.show(10)
    println(triples.count())
    triples
      .filter("p == 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'")
      .write.mode(SaveMode.Append).rdf("/tmp/lubm/out")



    val triplesRDD = session.rdf(lang)(path)
    triples.show(10)
    println(triples.count())
    triplesRDD
      .filter(_.predicateMatches(RDF.`type`.asNode()))
      .saveAsNTriplesFile("/tmp/lubm/out")
//
//    val triples = session.read.ntriples(path)
//
//    import session.implicits._
//
//    triples.show(10)
//    triples.select("s", "o").show(10)
//    println(triples.count())
//    println(triples.distinct().count())
//    triples.as("t1").join(triples.as("t2"), $"t1.o" === $"t2.s", "inner").show(10)

    session.stop()
  }
}
