package net.sansa_stack.rdf.spark.io

import org.apache.hadoop.io.LongWritable
import org.apache.jena.query.Dataset
import org.apache.spark.sql.SparkSession
import net.sansa_stack.rdf.common.io.hadoop.TrigFileInputFormat
import org.apache.hadoop.conf.Configuration

/**
 * A simple proof of concept main class for Trig reader.
 *
 * @author Lorenz Buehmann
 */
object TrigReader {

  def main(args: Array[String]): Unit = {
    if (args.length == 0) println("Usage: TrigReader <PATH_TO_FILE>")

    val path = args(0)

    val spark = SparkSession.builder
      // .master("local[4]")
      .appName("Trig reader")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // .config("spark.kryo.registrationRequired", "true")
      // .config("spark.eventLog.enabled", "true")
      //      .config("spark.kryo.registrator", String.join(", ",
      //      "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator"))
      .config("spark.default.parallelism", "4")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()

    val hadoopConf = new Configuration()

    // Sizes are somewhat skewed for bug-testing purposes
    hadoopConf.set("mapred.max.split.size", "10000321")
    hadoopConf.set("mapred.min.split.size", "10000321")

    val rdd = spark.sparkContext.newAPIHadoopFile(path, classOf[TrigFileInputFormat],
                                                  classOf[LongWritable], classOf[Dataset], hadoopConf)

    println(s"#Datasets: ${rdd.count()}")

  }
}
