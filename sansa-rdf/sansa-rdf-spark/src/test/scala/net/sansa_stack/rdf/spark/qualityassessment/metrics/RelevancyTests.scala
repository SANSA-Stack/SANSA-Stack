package net.sansa_stack.rdf.spark.qualityassessment.metrics

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import net.sansa_stack.rdf.spark.io._
import net.sansa_stack.rdf.spark.qualityassessment._
import org.apache.jena.riot.Lang
import org.scalatest.FunSuite

class RelevancyTests extends FunSuite with DataFrameSuiteBase {

  test("assessing the amount of triples should result in value 0.0") {

    val path = getClass.getResource("/rdf.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang)(path)

    val cnt = triples.assessAmountOfTriples()
    assert(cnt == 0.0)
  }

  test("assessing the coverage scope of a dataset should result in value 0.0") {

    val path = getClass.getResource("/rdf.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang)(path)

    val ratio = triples.assessCoverageScope()
    assert(ratio == 0.0)
  }

  test("assessing the coverage details of a dataset should restul in value 0.21") {

    val path = getClass.getResource("/rdf.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang)(path)

    val ratio = triples.assessCoverageDetail()
    assert(ratio == 0.21)
  }
}
