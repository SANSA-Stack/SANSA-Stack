package net.sansa_stack.owl.flink.dataset

import org.apache.flink.api.scala.ExecutionEnvironment
import org.scalatest.FunSuite


class FunctionalSyntaxOWLExpressionsDataSetBuilderTest extends FunSuite {
  lazy val env = ExecutionEnvironment.getExecutionEnvironment
  var _dataSet: OWLExpressionsDataSet = null
  def dataSet: OWLExpressionsDataSet = {
    if (_dataSet == null) {
      _dataSet = FunctionalSyntaxOWLExpressionsDataSetBuilder.build(
        env, this.getClass.getClassLoader.getResource("ont_functional.owl").getPath)
    }
    _dataSet
  }

  test("There should be three annotation lines with full URIs") {

    val res: List[String] = dataSet.filter(line => line.startsWith("Annotation(")).collect().toList
    val expected = List(
      "Annotation(<http://ex.com/foo#hasName> \"Name\")",
      "Annotation(<http://ex.com/bar#hasTitle> \"Title\")",
      """Annotation(<http://ex.com/default#description> "A longer
description running over
several lines")""")
    assert(res.size == 3)
    for (e <- expected) {
      assert(res.contains(e))
    }
  }

  /* Test disabled since OWLAPI will try to resolve imported ontology which
   * will fail or make the number of axioms unpredictable
   */
  //  test("There should be an import statement") {
  //    val res = rdd.filter(line => line.startsWith("Import")).collect()
  //    assert(res.length == 1)
  //    assert(res(0) == "Import(<http://www.example.com/my/2.0>)")
  //  }

  test("There should not be any empty lines") {
    val res = dataSet.filter(line => line.trim.isEmpty)
    assert(res.count() == 0)
  }

  test("There should not be any comment lines") {
    val res = dataSet.filter(line => line.trim.startsWith("#"))
    assert(res.count() == 0)
  }

  test("There should be a DisjointObjectProperties axiom") {
    val res = dataSet.filter(line => line.trim.startsWith("DisjointObjectProperties"))
    assert(res.count() == 1)
  }

  test("The total number of axioms should be correct") {
    val total = 70 // = 71 - uncommented Import(...)
    assert(dataSet.count() == total)
  }
}
