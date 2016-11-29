package net.sansa_stack.owl.spark.dataset

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite


class ManchesterSyntaxOWLExpressionsDatasetBuilderTest extends FunSuite with SharedSparkContext {
  lazy val spark = SparkSession.builder().appName(sc.appName).master(sc.master).getOrCreate()
  var _dataset: OWLExpressionsDataset = null
  def dataset = {
    if (_dataset == null) {
      _dataset = ManchesterSyntaxOWLExpressionsDatasetBuilder.build(
        spark, "src/test/resources/ont_manchester.owl")
//        spark, "hdfs://localhost:9000/ont_manchester.owl")
      _dataset.cache()
    }

    _dataset
  }

  test("There should be an annotations frame comprising three annotations with full URIs") {
    val res = dataset.filter(frame => frame.startsWith("Annotations:")).collect()
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
    assert(dataset.count() == total)
  }
}
