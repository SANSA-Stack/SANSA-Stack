package net.sansa_stack.rdf.spark.qualityassessment.metrics

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import net.sansa_stack.rdf.spark.io._
import net.sansa_stack.rdf.spark.qualityassessment._
import org.apache.jena.riot.Lang
import org.scalatest.funsuite.AnyFunSuite

class AvailabilityTests extends AnyFunSuite with DataFrameSuiteBase {

  test("getting the dereferenceable URIs should result in ratio 0.0") {

    val path = getClass.getResource("/rdf.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang)(path)

    val ratio = triples.assessDereferenceableUris()
    assert(ratio == 0.0)
  }

  test("getting the dereferenceable BackLinks should result in ratio 0.0") {

    val path = getClass.getResource("/rdf.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang)(path)

    val ratio = triples.assessDereferenceableBackLinks()
    assert(ratio == 0.0)
  }

  test("getting the dereferenceable ForwardLinks should result in ratio 0.0") {

    val path = getClass.getResource("/rdf.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang)(path)

    val ratio = triples.assessDereferenceableForwardLinks()
    assert(ratio == 0.0)
  }
}
