package net.sansa_stack.ml.spark.clustering

import org.apache.log4j.{ Level, Logger }
import org.apache.spark.broadcast.Broadcast
import scopt.OptionParser
import org.apache.spark.rdd.RDD
import scala.reflect.runtime.universe._
import org.apache.spark.{ SparkConf, SparkContext }

import scala.util.control.Breaks._
import org.apache.spark.sql.SparkSession

object RDFByModularityClusteringApp {

  case class Params(
      graphFile: String = null,//"src/main/resources/Clustering_sampledata.nt",
      outputFile: String = null,
      numIterations: Int = 100) extends AbstractParams[Params] {
  }
  abstract class AbstractParams[T: TypeTag] {

    private def tag: TypeTag[T] = typeTag[T]

    override def toString: String = {
      val tpe = tag.tpe
      val allAccessors = tpe.declarations.collect {
        case m: MethodSymbol if m.isCaseAccessor => m
      }
      val mirror = runtimeMirror(getClass.getClassLoader)
      val instanceMirror = mirror.reflect(this)
      allAccessors.map { f =>
        val paramName = f.name.toString
        val fieldMirror = instanceMirror.reflectField(f)
        val paramValue = fieldMirror.get
        s"  $paramName:\t$paramValue"
      }.mkString("{\n", ",\n", "\n}")
    }
  }
  def main(args: Array[String]) {

    val defaultParams = Params()

    val parser = new OptionParser[Params]("RDF By Modularity Clustering") {
      head("RDF By Modularity Clustering: an example RDF By Modularity Clustering app using RDF Graph.")

      opt[String]('i', "graphFile")
        .text(s"path to file that contains the input files (in N-Triple format)")
        .action((x, c) => c.copy(graphFile = x))

      opt[String]('o', "output")
        .text("the output directory")
        .action((x, c) => c.copy(outputFile = x))

      opt[Int]("numIterations")
        .text(s"number of iterations, default: ${defaultParams.numIterations}")
        .action((x, c) => c.copy(numIterations = x))

    }

    parser.parse(args, defaultParams).map { params =>
      run(params)
    }.getOrElse {
      sys.exit(1)
    }
  }

  def run(params: Params) {

    val sparkSession = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName(s"RDF By Modularity Clustering with $params")
      .getOrCreate()

    Logger.getRootLogger.setLevel(Level.ERROR)

    RDFByModularityClustering(sparkSession.sparkContext, params.numIterations, params.graphFile, params.outputFile)

    sparkSession.sparkContext.stop()
  }

}


