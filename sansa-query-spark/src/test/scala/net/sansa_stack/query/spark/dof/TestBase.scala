package net.sansa_stack.query.spark.dof

import net.sansa_stack.query.spark.dof.node.Helper
import net.sansa_stack.rdf.spark.io._
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.{ BeforeAndAfterAll, BeforeAndAfterAllConfigMap, ConfigMap, FlatSpec, Matchers }
import org.scalatest.junit.JUnitRunner

// @RunWith(classOf[JUnitRunner])
class TestBase extends FlatSpec with Matchers with BeforeAndAfterAllConfigMap /* BeforeAndAfterAll */ {
  private var _spark: SparkSession = _

  def spark: SparkSession = _spark

  // https://stackoverflow.com/questions/27781187/how-to-stop-info-messages-displaying-on-spark-console
  org.apache.log4j.Logger.getLogger("org").setLevel(org.apache.log4j.Level.OFF)
  org.apache.log4j.Logger.getLogger("akka").setLevel(org.apache.log4j.Level.OFF)

  def init(master: String = "", output_dir: String): Unit = {
    if (_spark == null) {
      _spark = SparkSession.builder
        .appName(s"DOF Analysis")
        .master(if (master == null || master.isEmpty) "local[*]" else master)
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    }

    Helper.init(output_dir)

    println("======================================")
    println("|             DOF Analysis           |")
    println("======================================")
  }

  override def afterAll(configMap: ConfigMap): Unit = {
    Helper.close
    spark.stop()
    println("Spark stopped")
  }
}
