package net.sansa_stack.owl.spark.dataset

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite


class FunctionalSyntaxOWLExpressionsDatasetBuilderTest extends FunSuite with SharedSparkContext {
  lazy val spark = SparkSession.builder().appName(sc.appName).master(sc.master).getOrCreate()
  var _dataset: OWLExpressionsDataset = null
  def dataset: OWLExpressionsDataset = {
    if (_dataset == null) {
      _dataset = FunctionalSyntaxOWLExpressionsDatasetBuilder.build(
        spark, this.getClass.getClassLoader.getResource("ont_functional.owl").getPath)
      _dataset.cache()
    }
    _dataset
  }

  test("There should be three annotation lines with full URIs") {
    val res = dataset.filter(line => line.startsWith("Annotation(")).collectAsList()
    val expected = List(
      "Annotation(<http://ex.com/foo#hasName> \"Name\")",
      "Annotation(<http://ex.com/bar#hasTitle> \"Title\")",
      """Annotation(<http://ex.com/default#description> "A longer
description running over
several lines")""")
    assert(res.size() == 3)
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
    val res = dataset.filter(line => line.trim.isEmpty)
    assert(res.count() == 0)
  }

  test("There should not be any comment lines") {
    val res = dataset.filter(line => line.trim.startsWith("#"))
    assert(res.count() == 0)
  }

  test("There should be a DisjointObjectProperties axiom") {
    val res = dataset.filter(line => line.trim.startsWith("DisjointObjectProperties"))
    assert(res.count() == 1)
  }

  test("The total number of axioms should be correct") {
    val total = 70 // = 71 - uncommented Import(...)
    assert(dataset.count() == total)
  }
}
