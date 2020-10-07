package net.sansa_stack.owl.spark.rdd

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

import net.sansa_stack.owl.spark.owl._

class FunctionalSyntaxOWLExpressionsRDDBuilderTest extends FunSuite with SharedSparkContext {
  lazy val spark = SparkSession.builder().appName(sc.appName).master(sc.master)
    .config(
      "spark.kryo.registrator",
      "net.sansa_stack.owl.spark.dataset.UnmodifiableCollectionKryoRegistrator")
    .getOrCreate()

  var _rdd: OWLExpressionsRDD = null
  val syntax = Syntax.FUNCTIONAL

  val filePath = this.getClass.getClassLoader.getResource("ont_functional.owl").getPath
  def rdd: OWLExpressionsRDD = {
    if (_rdd == null) {
      _rdd = spark.owlExpressions(syntax)(filePath)
      _rdd.cache()
    }

    _rdd
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
    val res = rdd.filter(line => line.trim.isEmpty).collect()
    assert(res.length == 0)
  }

  test("There should not be any comment lines") {
    val res = rdd.filter(line => line.trim.startsWith("#")).collect()
    assert(res.length == 0)
  }

  test("There should be a DisjointObjectProperties axiom") {
    val res = rdd.filter(_.trim.startsWith("DisjointObjectProperties")).collect()
    assert(res.length == 1)
  }

  test("The total number of axioms should be correct") {
    val total = 70 // = 71 - uncommented Import(...)
    assert(rdd.count() == total)
  }
}
