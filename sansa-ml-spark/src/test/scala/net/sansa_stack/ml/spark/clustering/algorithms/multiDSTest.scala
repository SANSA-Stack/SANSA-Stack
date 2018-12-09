package net.sansa_stack.ml.spark.clustering.algorithms

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite

class multiDSTest extends FunSuite with DataFrameSuiteBase {
  test("multiDS.multiDimensionScaling") {
    val testData = List((1.toLong, 2.toLong, 0.5), (1.toLong, 3.toLong, 1.0), (2.toLong, 3.toLong, 1.0))
    val testDataRDD: RDD[(Long, Long, Double)] = spark.sparkContext.parallelize(testData)
    val coordinates = new multiDS().multiDimensionScaling(testDataRDD, 3, 2)
    assert(coordinates.length === 3 && coordinates.head._2.length === 2)
  }
}

