package net.sansa_stack.examples.spark.ml.FPM

import scala.collection.mutable
import org.apache.spark.sql.SparkSession
import net.sansa_stack.rdf.spark.model.JenaSparkRDDOps
import net.sansa_stack.ml.spark.amieSpark.mining.KBObject.KB
import net.sansa_stack.ml.spark.amieSpark.mining.{RDFGraphLoader, DfLoader}
import net.sansa_stack.ml.spark.amieSpark.mining.MineRules.Algorithm

/*
 * Mine Rules
 * 
 */
object MineRules {

  def main(args: Array[String]) = {
    if (args.length < 2) {
      System.err.println(
        "Usage: Mine Rules <input>")
      System.exit(1)
    }
    val input = args(0)
    val optionsList = args.drop(2).map { arg =>
      arg.dropWhile(_ == '-').split('=') match {
        case Array(opt, v) => (opt -> v)
        case _             => throw new IllegalArgumentException("Invalid argument: " + arg)
      }
    }
    val options = mutable.Map(optionsList: _*)

    options.foreach {
      case (opt, _) => throw new IllegalArgumentException("Invalid option: " + opt)
    }
    println("======================================")
    println("|        Mines the Rules example     |")
    println("======================================")

    val sparkSession = SparkSession.builder
      .master("local[*]")
      .appName(" Mines the Rules example (" + input + ")")
      .getOrCreate()

    val ops = JenaSparkRDDOps(sparkSession.sparkContext)
    import ops._

    val know = new KB()
    know.setKbSrc(input)
    know.setKbGraph(RDFGraphLoader.loadFromFile(know.getKbSrc(), sparkSession.sparkContext, 2))
    know.setDFTable(DfLoader.loadFromFileDF(know.getKbSrc, sparkSession.sparkContext, sparkSession.sqlContext, 2))
    val algo = new Algorithm(know, 0.01, 3, 0.1)

    var erg = algo.ruleMining(sparkSession.sparkContext, sparkSession.sqlContext)
    println(erg)

    sparkSession.stop

  }

}