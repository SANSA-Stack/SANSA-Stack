package net.sansa_stack.rdf.spark.io

import java.net.URI

import net.sansa_stack.rdf.common.io.hadoop.rdf.trig.FileInputFormatTrigDataset
import net.sansa_stack.rdf.common.io.hadoop.trash.TrigFileInputFormatOld
import org.apache.hadoop.io.LongWritable
import org.apache.jena.query.Dataset
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._

/**
 * A simple proof of concept main class for Trig reader.
 *
 * @note Hadoop specific config options for the Trig reader can be set by prepending
 *
 * {{{spark.hadoop.}}}
 *
 * to the config params.
 *
 * Currently, we do support
 * {{{
 * mapreduce.input.trigrecordreader.record.maxlength
 * mapreduce.input.trigrecordreader.record.minlength
 * mapreduce.input.trigrecordreader.probe.count
 * mapreduce.input.trigrecordreader.prefixes.maxlength
 * }}}
 *
 *
 * @author Lorenz Buehmann
 */
object TrigReader {

  case class Config(
                     in: URI = null,
                     mode: String = "",
                     sampleSize: Int = 10)

  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[Config]("Trig Reader") {

      head("Trig Reader", "0.7")

      cmd("quads")
        .text("compute number of quads")
        .action((x, c) => c.copy(mode = "quads"))

      cmd("graphs")
        .text("compute number of graphs")
        .action((x, c) => c.copy(mode = "graphs"))

      cmd("sample")
        .text("show sample of quads")
        .action((x, c) => c.copy(mode = "sample"))
        .children(
          opt[Int]("size")
            .abbr("n")
            .action((x, c) => c.copy(sampleSize = x))
            .text("sample size (too high number can be slow or lead to memory issues)"),
          checkConfig(
            c =>
              if (c.mode == "sample" && c.sampleSize <= 0) failure("sample size must be > 0")
              else success)
        )

      arg[URI]("<file>")
        .action((x, c) => c.copy(in = x))
        .text("URI to N-Quad file to process")
        .valueName("<file>")
        .required()

    }

    // parser.parse returns Option[C]
    parser.parse(args, Config()) match {
      case Some(config) =>
        val spark = SparkSession.builder
          .appName("Trig reader")
          .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .getOrCreate()

        val hadoopConf = spark.sparkContext.hadoopConfiguration

        // Sizes are somewhat skewed for bug-testing purposes
        // hadoopConf.set("mapred.max.split.size", "10000321")
        // hadoopConf.set("mapred.min.split.size", "10000321")

        val rdd = spark.sparkContext.newAPIHadoopFile(config.in.getPath, classOf[FileInputFormatTrigDataset],
          classOf[LongWritable], classOf[Dataset], hadoopConf)

        config.mode match {
          case "graphs" => println(s"#parsed graphs: ${rdd.count()}")
          case "quads" => println(s"#parsed quads: ${rdd.map(_._2).map(_.asDatasetGraph()).flatMap(_.find().asScala).count()}")
          case "sample" => println(s"max ${config.sampleSize} sample quads:\n" + rdd.take(config.sampleSize).map {
            _.toString.replaceAll("[\\x00-\\x1f]", "???")
          }.mkString("\n"))
        }

        spark.stop()

      case None =>
      // arguments are bad, error message will have been displayed
    }
  }
}
