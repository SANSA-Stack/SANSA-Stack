package net.sansa_stack.owl.spark.dataset

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.Dataset
import org.scalatest.FunSuite


class FunctionalSyntaxOWLFileDatasetTest extends FunSuite with SharedSparkContext {
  var _dataset: Dataset[String] = null

  def dataset = {
    if (_dataset == null) {
      _dataset = FunctionalSyntaxOWLFileDataset(sc, "src/test/resources/ont_functional.owl", sc.defaultMinPartitions)
    }

    _dataset
  }

  test("There should be three annotation lines with full URIs") {
    val res = dataset.filter(line => line.startsWith("Annotation(")).collect()
    val expected = List(
      "Annotation(<http://ex.com/foo#hasName> \"Name\")",
      "Annotation(<http://ex.com/bar#hasTitle> \"Title\")",
      """Annotation(<http://ex.com/default#description> "A longer
description runnig over
several lines")""")

    assert(res.length == 3)
    for (e <- expected) {
      assert(res.contains(e))
    }
  }

  /* Test disabled since OWLAPI will try to resolve imported ontology which
   * will fail or make the number of axioms unpredictable
   */
//  test("There should be an import statement") {
//    val res = dataset.filter(line => line.startsWith("Import")).collect()
//    assert(res.length == 1)
//    assert(res(0) == "Import(<http://www.example.com/my/2.0>)")
//  }

  test("There should not be any empty lines") {
    val res = dataset.filter(line => line.trim.isEmpty).collect()
    assert(res.length == 0)
  }

  test("There should not be any comment lines") {
    val res = dataset.filter(line => line.trim.startsWith("#")).collect()
    assert(res.length == 0)
  }

  test("There should be a DisjointObjectProperties axiom") {
    val res = dataset.filter(line => line.trim.startsWith("DisjointObjectProperties")).collect()
    assert(res.length == 1)
  }

  test("The total number of axioms should be correct") {
    val total = 70 // = 71 - uncommented Import(...)
    assert(dataset.count() == total)
  }
}
