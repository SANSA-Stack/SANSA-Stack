package net.sansa_stack.ml.spark.clustering.algorithms

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite

class PICTest extends FunSuite with DataFrameSuiteBase {
  test("PIC.picSparkML") {
    val testData = List((1.toLong, 2.toLong, 1.0), (1.toLong, 3.toLong, 0.0), (2.toLong, 3.toLong, 0.0))
    val testDataRDD: RDD[(Long, Long, Double)] = spark.sparkContext.parallelize(testData)
    val clusters = new PIC().picSparkML(testDataRDD, 2, 1, sparkSession = spark)
    assert(clusters.size === 2)
  }
}

