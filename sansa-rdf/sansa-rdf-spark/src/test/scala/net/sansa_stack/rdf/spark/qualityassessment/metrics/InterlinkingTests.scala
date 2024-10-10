package net.sansa_stack.rdf.spark.qualityassessment.metrics

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import net.sansa_stack.rdf.spark.io._
import net.sansa_stack.rdf.spark.qualityassessment._
import org.apache.jena.riot.Lang
import org.scalatest.funsuite.AnyFunSuite

class InterlinkingTests extends AnyFunSuite with DataFrameSuiteBase {
  test("assessing the external SameAs links should result in value 0.0") {

    val path = getClass.getResource("/rdf.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang)(path)

    val value = triples.assessExternalSameAsLinks()
    assert(value == 0.0)
  }
}
