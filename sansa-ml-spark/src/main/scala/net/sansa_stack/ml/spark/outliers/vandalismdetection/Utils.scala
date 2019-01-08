package net.sansa_stack.ml.spark.outliers.vandalismdetection

import org.apache.spark.ml.linalg.{ Vector, Vectors }

object Utils extends Serializable {

  def arrayString2VectorDouble(str: String): Vector = {

    val vector: Vector = Vectors.zeros(0)
    val str_recordList: Array[String] = str.split(",")
    val size: Integer = str_recordList.size
    var double_recordList: Array[Double] = new Array[Double](size)

    // val x= str_recordList.toVector

    for (record <- str_recordList) {

      if (record.nonEmpty) {

        val tem0: String = record.replace("[", "").trim()
        val tem1: String = tem0.replace("]", "").trim()

        val tem2 = tem1.toDouble
        double_recordList +:= tem2

      }

    }

    toVector(double_recordList)

  }

  def toVector(arra: Array[Double]): Vector =
    Vectors.dense(arra)

}