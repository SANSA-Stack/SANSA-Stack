package net.sansa_stack.rdf.spark.io.benchmark.quad

import java.net.URI

import com.ibm.sparktc.sparkbench.utils.GeneralFunctions._
import com.ibm.sparktc.sparkbench.utils.{SaveModes, SparkBenchException}
import com.ibm.sparktc.sparkbench.workload.{Workload, WorkloadDefaults}
import net.sansa_stack.hadoop.format.jena.trig.FileInputFormatRdfTrigDataset
import net.sansa_stack.rdf.spark.io.nquads.NQuadReader
import org.aksw.jenax.arq.dataset.api.DatasetOneNg
import org.apache.hadoop.io.LongWritable
import org.apache.jena.query.Dataset
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.jdk.CollectionConverters._


/**
 * @author Lorenz Buehmann
 */
object QuadParsing extends WorkloadDefaults {
  override val name: String = "Quad parsing workload"

  // keys in map have been toLowerCase()'d for consistency
  override def apply(m: Map[String, Any]): Workload =
    QuadParsingWorkload(input = Option(getOrThrowT[String](m, "input")),
                        format = getOrThrowT[String](m, "format"))
}


case class QuadParsingCountResult(name: String,
                                  format: String,
                                  timestamp: Long,
                                  totalRuntime: Long,
                                  numQuads: Long)

case class QuadParsingWorkload(input: Option[String] = None,
                               output: Option[String] = None,
                               format: String,
                               saveMode: String = SaveModes.error)
  extends Workload {

  override def doWorkload(df: Option[DataFrame], spark: SparkSession): DataFrame = {
    val startTime = System.currentTimeMillis()

    val (runtime, numQuads) = time {
      val rdd = format match {
        case "nquad" => NQuadReader.load(spark, URI.create(input.get))
        case "trig" =>
          val hadoopConf = spark.sparkContext.hadoopConfiguration
          spark.sparkContext.newAPIHadoopFile(input.get, classOf[FileInputFormatRdfTrigDataset],
            classOf[LongWritable], classOf[DatasetOneNg], hadoopConf)
            .map(_._2)
            .flatMap(_.asDatasetGraph().find().asScala)
        case other: String => throw SparkBenchException(s"Unsupported data format for quads: $other")
      }
      rdd.count()
    }

    spark.createDataFrame(Seq(QuadParsingCountResult(QuadParsing.name, format, startTime, runtime, numQuads)))
  }
}
