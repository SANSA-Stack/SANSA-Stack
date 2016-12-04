package net.sansa_stack.ml.spark.clustering

import scala.reflect.runtime.universe._
import scopt.OptionParser
import org.apache.log4j.{ Level, Logger }
import org.apache.spark.mllib.util.MLUtils
import java.io.{ FileReader, FileNotFoundException, IOException }
import org.apache.spark.mllib.linalg.Vectors
import java.lang.{ Long => JLong }
import java.lang.{ Long => JLong }
import breeze.linalg.{ squaredDistance, DenseVector, Vector }
import org.apache.spark.sql.SparkSession

object RDFGraphPICApp {

  case class Params(
      input: String = null,
      k: Int = 100,
      maxIterations: Int = 400) extends AbstractParams[Params] {
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

    val parser = new OptionParser[Params]("PowerIterationClusteringExample") {
      head("PowerIterationClusteringExample: an example PIC app using concentric circles.")

      opt[Int]('k', "k")
        .text(s"number of circles (/clusters), default: ${defaultParams.k}")
        .action((x, c) => c.copy(k = x))

      opt[Int]("maxIterations")
        .text(s"number of iterations, default: ${defaultParams.maxIterations}")
        .action((x, c) => c.copy(maxIterations = x))

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
      .appName(s"PowerIterationClustering with $params")
      .getOrCreate()

    Logger.getRootLogger.setLevel(Level.WARN)

    val data = sparkSession.sparkContext.textFile("/home/tina/sample.txt")

    data.collect().foreach(println)

    val model = RDFGraphClustering(data, params.k, params.maxIterations).run()
    
    val clusters = model.assignments.collect().groupBy(_.cluster).mapValues(_.map(_.id))
    val assignments = clusters.toList.sortBy { case (k, v) => v.length }
    val assignmentsStr = assignments
      .map {
        case (k, v) =>
          s"$k -> ${v.sorted.mkString("[", ",", "]")}"
      }.mkString(",")
    val sizesStr = assignments.map {
      _._2.size
    }.sorted.mkString("(", ",", ")")
    println(s"Cluster assignments: $assignmentsStr\ncluster sizes: $sizesStr")

    sparkSession.sparkContext.stop()
  }

}

