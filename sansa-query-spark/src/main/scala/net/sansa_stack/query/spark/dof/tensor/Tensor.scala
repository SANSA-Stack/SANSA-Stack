package net.sansa_stack.query.spark.dof.tensor

import net.sansa_stack.query.spark.dof.node.SPO
import net.sansa_stack.query.spark.dof.triple.Reader
import org.apache.jena.graph.{ Node, Triple }
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import scala.reflect.ClassTag

abstract class Tensor[R, N: ClassTag, T, A](spark: SparkSession, reader: Reader)
  extends TensorApi[R, N, T, A] {

  private val _data = readData
  def data: R = _data

  private val _spo = buildSPO
  def spo: SPO[Node] = _spo

  private val _tensor = buildTensor
  def tensor: T = _tensor

  def sparkContext: SparkContext = spark.sparkContext

  def isRDDEquals(expected: RDD[String], result: RDD[String]): Boolean = {
    // return expected.subtract(result).isEmpty
    return compareRDD(expected, result).isEmpty
  }

  private def compareRDD(expected: RDD[String], result: RDD[String]): Option[(String, Int, Int)] = {

    // Key the values and count the number of each unique element
    val expectedKeyed = expected.map(x => (x, 1)).reduceByKey(_ + _)
    val resultKeyed = result.map(x => (x, 1)).reduceByKey(_ + _)
    // Group them together and filter for difference
    expectedKeyed.cogroup(resultKeyed).filter {
      case (_, (i1, i2)) =>
        i1.isEmpty || i2.isEmpty || i1.head != i2.head
    }
      .take(1).headOption.
      map {
        case (v, (i1, i2)) =>
          (v, i1.headOption.getOrElse(0), i2.headOption.getOrElse(0))
      }
  }
}
