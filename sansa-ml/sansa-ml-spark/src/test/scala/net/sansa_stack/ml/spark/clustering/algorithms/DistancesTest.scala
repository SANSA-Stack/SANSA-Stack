package net.sansa_stack.ml.spark.clustering.algorithms

import org.scalatest.FunSuite

class DistancesTest extends FunSuite {
  val testData1 = Set("a", "b")
  val testData2 = Set("b", "c", "d")

  test("Distances.jaccardSimilarity") {
    assert(new Distances().jaccardSimilarity(testData1, testData2) === 0.25)
  }
}

