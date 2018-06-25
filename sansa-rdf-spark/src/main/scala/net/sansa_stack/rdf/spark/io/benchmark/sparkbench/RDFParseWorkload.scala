package net.sansa_stack.rdf.spark.io.benchmark.sparkbench

import java.io.ByteArrayInputStream
import java.net.URI

import com.ibm.sparktc.sparkbench.utils.GeneralFunctions._
import com.ibm.sparktc.sparkbench.utils.SaveModes
import com.ibm.sparktc.sparkbench.workload.{ Workload, WorkloadDefaults }
import net.sansa_stack.rdf.benchmark.io.ReadableByteChannelFromIterator
import org.apache.jena.graph.Triple
import org.apache.jena.riot.{ Lang, RDFDataMgr }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ DataFrame, SparkSession }

/**
 * A custom Spark-Bench workload for the RDF N-Triples reader.
 * The goal is to measure the runtime of the RDF reader, i.e. the performance of the Jena-based parser.
 *
 * It loads the RDF file from disk and does a `count` as action.
 *
 * @author Lorenz Buehmann
 */
object RDFParseWorkload extends WorkloadDefaults {

  override val name: String = "rdf-parse"

  override def apply(m: Map[String, Any]): Workload =
    new RDFParseWorkload(
      input = m.get("input").map(_.asInstanceOf[String]),
      output = m.get("output").map(_.asInstanceOf[String]),
      saveMode = getOrDefault[String](m, "save-mode", SaveModes.error),
      numPartitions = m.get("partitions").map(_.asInstanceOf[Int]),
      mode = m.get("mode").map(_.asInstanceOf[String]))

}

case class RDFParseWorkload(
  input: Option[String],
  output: Option[String] = None,
  saveMode: String,
  numPartitions: Option[Int] = None,
  mode: Option[String] = None)
  extends Workload {

  def parse(spark: SparkSession): (Long, Long) = time {
    if (mode.get == "old") {
      loadSimple(spark, Seq(URI.create(input.get)), numPartitions.get).count()
    } else {
      loadFast(spark, Seq(URI.create(input.get)), numPartitions.get).count()
    }
  }

  /**
   * Loads N-Triples data from a set of files or directories into an RDD.
   * The path can also contain multiple paths
   * and even wildcards, e.g.
   * `"/my/dir1,/my/paths/part-00[0-5]*,/another/dir,/a/specific/file"`
   *
   * @param session the Spark session
   * @param paths   the path to the N-Triples file(s)
   * @param minPartitions suggested minimum number of partitions for the resulting RDD
   * @return the RDD of triples
   */
  private def loadFast(session: SparkSession, paths: Seq[URI], minPartitions: Int = 20): RDD[Triple] = {
    import scala.collection.JavaConverters._
    session.sparkContext
      .textFile(paths.mkString(","), minPartitions)
      .mapPartitions(p => RDFDataMgr.createIteratorTriples(ReadableByteChannelFromIterator.toInputStream(p.asJava), Lang.NTRIPLES, null).asScala)
  }

  /**
   * Loads N-Triples data from a set of files or directories into an RDD.
   * The path can also contain multiple paths
   * and even wildcards, e.g.
   * `"/my/dir1,/my/paths/part-00[0-5]*,/another/dir,/a/specific/file"`
   *
   * @param session the Spark session
   * @param paths   the path to the N-Triples file(s)
   * @return the RDD of triples
   */
  private def loadSimple(session: SparkSession, paths: Seq[URI], minPartitions: Int = 20): RDD[Triple] = {
    session.sparkContext
      .textFile(paths.mkString(","), minPartitions)
      .filter(l => !l.startsWith("#") && !l.isEmpty)
      .map(l => RDFDataMgr.createIteratorTriples(new ByteArrayInputStream(l.getBytes), Lang.NTRIPLES, null).next)
  }

  override def doWorkload(df: Option[DataFrame], spark: SparkSession): DataFrame = {
    val timestamp = System.currentTimeMillis()

    val (runtime, numTriples) = parse(spark)

    spark.createDataFrame(Seq(
      RDFParseWorkloadResult(
        "rdf-parse",
        timestamp,
        runtime,
        numTriples,
        mode.get)))
  }
}

case class RDFParseWorkloadResult(
  name: String,
  timestamp: Long,
  runtime: Long,
  numTriples: Long,
  mode: String)
