package net.sansa_stack.examples.spark.ml.mining

import scala.collection.mutable
import org.apache.spark.sql.SparkSession
import net.sansa_stack.ml.spark.mining.amieSpark.KBObject.KB
import net.sansa_stack.ml.spark.mining.amieSpark.{ RDFGraphLoader, DfLoader }
import net.sansa_stack.ml.spark.mining.amieSpark.MineRules.Algorithm
import net.sansa_stack.ml.spark.mining.amieSpark.MineRules.Algorithm

/*
 * Mine Rules
 *
 */
object MineRules {

  def main(args: Array[String]) {
    parser.parse(args, Config()) match {
      case Some(config) =>
        run(config.in, config.out)
      case None =>
        println(parser.usage)
    }
  }

  def run(input: String, outputPath: String): Unit = {

    println("======================================")
    println("|        Mines the Rules example     |")
    println("======================================")

    val spark = SparkSession.builder
      .appName(s" Mines the Rules example ( $input )")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
      
    val hdfsPath = outputPath + "/"

    val know = new KB()
    know.sethdfsPath(hdfsPath)
    know.setKbSrc(input)

    know.setKbGraph(RDFGraphLoader.loadFromFile(know.getKbSrc(), spark.sparkContext, 2))
    know.setDFTable(DfLoader.loadFromFileDF(know.getKbSrc, spark.sparkContext, spark.sqlContext, 2))

    val algo = new Algorithm(know, 0.01, 3, 0.1, hdfsPath)

    //var erg = algo.ruleMining(sparkSession.sparkContext, sparkSession.sqlContext)
    //println(erg)
    var output = algo.ruleMining(spark.sparkContext, spark.sqlContext)

    var outString = output.map { x =>
      var rdfTrp = x.getRule()
      var temp = ""
      for (i <- 0 to rdfTrp.length - 1) {
        if (i == 0) {
          temp = rdfTrp(i) + " <= "
        } else {
          temp += rdfTrp(i) + " \u2227 "
        }
      }
      temp = temp.stripSuffix(" \u2227 ")
      temp
    }.toSeq
    var rddOut = spark.sparkContext.parallelize(outString).repartition(1)

    rddOut.saveAsTextFile(outputPath + "/testOut")
  }

  case class Config(
    in:  String = "",
    out: String = "")

  val parser = new scopt.OptionParser[Config]("Mines the Rules example") {

    head("Mines the Rules example")

    opt[String]('i', "input").required().valueName("<path>").
      action((x, c) => c.copy(in = x)).
      text("path to file that contains the data")

    opt[String]('o', "out").required().valueName("<directory>").
      action((x, c) => c.copy(out = x)).
      text("the output directory")

    help("help").text("prints this usage text")
  }
}