package net.sansa_stack.owl.spark.rdd

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.FunSuite


class ManchesterSyntaxOWLExpressionsRDDBuilderTest extends FunSuite with SharedSparkContext {
  var _rdd: OWLExpressionsRDD = null

  def rdd = {
    if (_rdd == null) {
      _rdd = ManchesterSyntaxOWLExpressionsRDDBuilder.build(
        sc, "src/test/resources/ont_manchester.owl")
//        sc, "hdfs://localhost:9000/ont_manchester.owl")
      _rdd.cache()
    }

    _rdd
  }

  test("There should be an annotations frame comprising three annotations with full URIs") {
    val res = rdd.filter(frame => frame.startsWith("Annotations:")).collect()
    val expected = "Annotations: \n" +
      "    <http://ex.com/bar#hasTitle> \"Title\",\n" +
      "    description \"A longer\n" +
      "description running over\n" +
      "several lines\",\n" +
      "    <http://ex.com/foo#hasName> \"Name\""

    assert(res.length == 1)
    assert(res(0).trim == expected)
  }

  test("The total number of frames should be correct") {
    val total = 50
    assert(rdd.count() == total)
  }
}
