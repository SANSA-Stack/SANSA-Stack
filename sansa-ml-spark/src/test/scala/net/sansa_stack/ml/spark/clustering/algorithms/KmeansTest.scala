package net.sansa_stack.ml.spark.clustering.algorithms

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite

class KmeansTest extends FunSuite with DataFrameSuiteBase {
  test("Kmeans.mdsEncoding") {
    val mdsTestData = spark.sparkContext.parallelize(List((1.toLong, 2.toLong, 0.5), (1.toLong, 3.toLong, 1.0), (2.toLong, 3.toLong, 1.0)))
    mdsTestData.foreach(println)
    // val mdsTestDataRDD: RDD[(Long, Long, Double)] = sc.parallelize(mdsTestData)
    val (mdsEncodedDF, mdsEncoded) = new Encoder().mdsEncoding(mdsTestData, 3, 2, spark)
    val km_result = new Kmeans().kmClustering(2, 2, mdsEncodedDF, spark)
    assert(km_result.size === 2) // 2 clusters
  }
}

