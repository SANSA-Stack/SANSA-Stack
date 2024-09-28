package net.sansa_stack.inference.spark.utils

import java.net.URI

import net.sansa_stack.inference.spark.utils.RDFTriple
import net.sansa_stack.inference.spark.data.loader.RDFGraphLoader
import org.apache.jena.graph.Node
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

case class RDFTriple(s: String, p: String, o: String)

/**
  * Convert N-Triples data into the [[https://parquet.apache.org/ Apache Parquet format]].
  *
  * @author Lorenz Buehmann
  */
class NTriplesToParquetConverter(val session: SparkSession) {

  import session.implicits._

  private implicit def pathURIsConverter(uris: Seq[URI]): String = uris.map(p => p.toString()).mkString(",")

  def saveAsParquet(inputPath: URI, outputPath: URI): Unit = {
    saveAsParquet(Seq(inputPath), outputPath)
  }

  def saveAsParquet(inputPaths: scala.Seq[URI], outputPath: URI): Unit = {
    // load the RDD
    val triplesRDD: RDD[RDFTriple] = RDFGraphLoader.loadFromDisk(session, inputPaths).triples
      .map(t => RDFTriple(t.getSubject.toString(), t.getPredicate.toString(), t.getObject.toString()))

    // generate a Dataset
    val tripleDS = session.createDataset(triplesRDD)

    // write to disk in Parquet format
    tripleDS
//      .repartition(tripleDS("s"))
      .write.mode(SaveMode.Append).parquet(outputPath.toString())
  }
}

object NTriplesToParquetConverter {

  val DEFAULT_PARALLELISM = 200
  val DEFAULT_NUM_THREADS = 4

  def main(args: Array[String]): Unit = {
    if (args.length < 2) sys.error("USAGE: NTriplesToParquetConverter <INPUT_PATH>+ <OUTPUT_PATH> <NUM_THREADS>? <PARALLELISM>?")

    val inputPaths = args(0).split(",").map(URI.create)
    val outputPath = URI.create(args(1))
    val numThreads = if (args.length > 2)  args(2).toInt else DEFAULT_NUM_THREADS
    val parallelism = if (args.length > 3)  args(3).toInt else DEFAULT_PARALLELISM


    // the SPARK config
    val session = SparkSession.builder
      .appName("N-Triples to Parquet Conversion")
      .master(s"local[$numThreads]")
      .config("spark.eventLog.enabled", "true")
      .config("spark.hadoop.validateOutputSpecs", "false") // override output files
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.default.parallelism", parallelism)
      .config("spark.ui.showConsoleProgress", "false")
      .config("spark.sql.shuffle.partitions", parallelism)
      .config("parquet.enable.summary-metadata", "false")
      .getOrCreate()

    new NTriplesToParquetConverter(session).saveAsParquet(inputPaths, outputPath)

    session.stop()
  }


}
