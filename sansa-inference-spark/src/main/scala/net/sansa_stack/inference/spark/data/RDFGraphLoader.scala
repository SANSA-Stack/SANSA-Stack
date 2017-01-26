package net.sansa_stack.inference.spark.data

import java.io.File

import org.apache.jena.rdf.model.Resource
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import net.sansa_stack.inference.data.RDFTriple
import net.sansa_stack.inference.utils.NTriplesStringToRDFTriple

/**
  * Load an RDF graph from disk.
  *
  * @author Lorenz Buehmann
  *
  */
object RDFGraphLoader {

  private val logger = com.typesafe.scalalogging.Logger(LoggerFactory.getLogger(this.getClass.getName))

  implicit def filesConverter(files: Seq[File]): String = files.map(p => p.getAbsolutePath).mkString(",")

  /**
    * Load an RDF graph from a file or directory. The path can also contain multiple paths
    * and even wildcards, e.g.
    * "/my/dir1,/my/paths/part-00[0-5]*,/another/dir,/a/specific/file"
    *
    * @param path the absolute path of the file
    * @param session the Spark session
    * @param minPartitions min number of partitions for Hadoop RDDs ([[SparkContext.defaultMinPartitions]])
    * @return an RDF graph
    */
  def loadFromFile(path: String, session: SparkSession, minPartitions: Int = 2): RDFGraph = {
    logger.info("loading triples from disk...")
    val startTime = System.currentTimeMillis()

    val triples = session.sparkContext
      .textFile(path, minPartitions) // read the text file
      .map(new NTriplesStringToRDFTriple()) // convert to triple object

//  logger.info("finished loading " + triples.count() + " triples in " + (System.currentTimeMillis()-startTime) + "ms.")
    new RDFGraph(triples)
  }

  /**
    * Load an RDF graph from multiple files.
    *
    * @param paths the files
    * @param session the Spark session
    * @param minPartitions min number of partitions for Hadoop RDDs ([[SparkContext.defaultMinPartitions]])
    * @return an RDF graph
    */
  def loadFromDisk(paths: Seq[File], session: SparkSession, minPartitions: Int = 2): RDFGraph = {
    logger.info("loading triples from disk...")
    val startTime = System.currentTimeMillis()

    val pathsConcat = paths.map(p => p.getAbsolutePath).mkString(",") // make concatenated string of paths

    val triples = session.sparkContext
      .textFile(pathsConcat, minPartitions) // read the text files
      .map(new NTriplesStringToRDFTriple()) // convert to triple object
//      .repartition(minPartitions)

    // logger.info("finished loading " + triples.count() + " triples in " +
    // (System.currentTimeMillis()-startTime) + "ms.")
    new RDFGraph(triples)
  }

  /**
    * Load an RDF graph from a file or directory. The path can also contain multiple paths
    * and even wildcards, e.g.
    * "/my/dir1,/my/paths/part-00[0-5]*,/another/dir,/a/specific/file"
    *
    * @param path the files
    * @param session the Spark session
    * @param minPartitions min number of partitions for Hadoop RDDs ([[SparkContext.defaultMinPartitions]])
    * @return an RDF graph
    */
  def loadGraphFromFile(path: String, session: SparkSession, minPartitions: Int = 2): RDFGraphNative = {
    logger.info("loading triples from disk...")
    val startTime = System.currentTimeMillis()

    val triples = session.sparkContext
      .textFile(path, minPartitions) // read the text file
      .map(new NTriplesStringToRDFTriple()) // convert to triple object

    // logger.info("finished loading " + triples.count() + " triples in " +
    // (System.currentTimeMillis()-startTime) + "ms.")
    new RDFGraphNative(triples)
  }

  case class RDFTriple2(s: String, p: String, o: String)

  def loadGraphFromDiskAsDataset(implicit session: SparkSession, paths: scala.Seq[File]): RDFGraphDataset = {
    logger.info("loading triples from disk...")
    val startTime = System.currentTimeMillis()

    import org.apache.spark.sql.functions._
    val splitter = udf((str: String) => {
      val splitted = str.split(" ").lift
      Array(splitted(0), splitted(1), splitted(2))
    })

    implicit val myObjEncoder = org.apache.spark.sql.Encoders.kryo[RDFTriple]
    val spark = session.sqlContext
    import spark.implicits._

    val triples = session.read
      .textFile(paths) // read the text file
      .select(splitter($"value") as "tokens")
      .select($"tokens"(0) as "s", $"tokens"(1) as "p", $"tokens"(2) as "o")
      .as[RDFTriple]


    // convert to triple object

    // logger.info("finished loading " + triples.count() + " triples in " +g
    // (System.currentTimeMillis()-startTime) + "ms.")
    new RDFGraphDataset(triples)
  }

  def loadGraphDataFrameFromFile(path: String, session: SparkSession, minPartitions: Int = 2): RDFGraphDataFrame = {
    new RDFGraphDataFrame(loadGraphFromFile(path, session, minPartitions).toDataFrame(session))
  }
}
