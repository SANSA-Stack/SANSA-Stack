package net.sansa_stack.ml.spark.clustering



import scala.reflect.runtime.universe._
import scopt.OptionParser
import org.apache.log4j.{ Level, Logger }
import org.apache.spark.sql.SparkSession

object BorderFlowApp {
    
    val Input ="hdfs://172.18.160.17:54310/TinaBoroukhian/input/dbpedia.txt"
    
  case class Params(
      input: String = Input) extends AbstractParams[Params] {
  }
  abstract class AbstractParams[T: TypeTag] {

    private def tag: TypeTag[T] = typeTag[T]

    override def toString: String = {
      val tpe = tag.tpe
      val allAccessors = tpe.decls.collect {
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

    val parser = new OptionParser[Params]("BorderFlow") {
      head("BorderFlow: an example BorderFlow app.")

      opt[String]('i', "input")
        .text(s"path to file contains the input files, default: ${defaultParams.input}")
        .action((x, c) => c.copy(input = x))

    }

    parser.parse(args, defaultParams).map { params =>
      run(params)
    }.getOrElse {
      sys.exit(1)
    }
  }

  def run(params: Params) {

    val spark = SparkSession.builder
      .master("spark://172.18.160.16:3077")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName(s"BorderFlow with $params")
      .getOrCreate()

    Logger.getRootLogger.setLevel(Level.WARN)

    BorderFlow(spark, params.input)

    spark.stop()
  }

}
