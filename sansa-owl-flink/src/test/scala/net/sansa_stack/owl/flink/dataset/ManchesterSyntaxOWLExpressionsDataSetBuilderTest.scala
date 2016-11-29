package net.sansa_stack.owl.flink.dataset

import org.apache.flink.api.scala.ExecutionEnvironment
import org.scalatest.FunSuite


class ManchesterSyntaxOWLExpressionsDataSetBuilderTest extends FunSuite {
  lazy val env = ExecutionEnvironment.getExecutionEnvironment
  var _dataSet: OWLExpressionsDataSet = null
  def dataSet: OWLExpressionsDataSet = {
    if (_dataSet == null) {
      _dataSet = ManchesterSyntaxOWLExpressionsDataSetBuilder.build(
        env, "src/test/resources/ont_manchester.owl")
//        env, "hdfs://localhost:9000/ont_manchester.owl")
    }
    _dataSet
  }

  test("There should be an annotations frame comprising three annotations with full URIs") {
    val res = dataSet.filter(frame => frame.startsWith("Annotations:")).collect()
    val expected = "Annotations: \n" +
      "    <http://ex.com/bar#hasTitle> \"Title\",\n" +
      "    description \"A longer\n" +
      "description running over\n" +
      "several lines\",\n" +
      "    <http://ex.com/foo#hasName> \"Name\""

    assert(res.size == 1)
    assert(res(0).trim == expected)
  }

  test("The total number of frames should be correct") {
    val total = 50
    assert(dataSet.count() == total)
  }
}
