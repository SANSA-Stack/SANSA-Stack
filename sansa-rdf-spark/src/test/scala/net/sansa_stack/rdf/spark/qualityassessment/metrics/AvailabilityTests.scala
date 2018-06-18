package net.sansa_stack.rdf.spark.qualityassessment.metrics

import org.scalatest.FunSuite
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.jena.riot.Lang
import net.sansa_stack.rdf.spark.io._
import net.sansa_stack.rdf.spark.qualityassessment._

class AvailabilityTests extends FunSuite with DataFrameSuiteBase {

  test("getting the dereferenceable URIs should result in ratio 0.1320754716981132") {

    val path = getClass.getResource("/rdf.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang)(path)

    val ratio = triples.assessDereferenceableUris()
    assert(ratio == 0.1320754716981132)
  }
  
  test("getting the dereferenceable BackLinks should result in ratio 0.15384615384615385") {

    val path = getClass.getResource("/rdf.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang)(path)

    val ratio = triples.assessDereferenceableBackLinks()
    assert(ratio == 0.15384615384615385)
  }
  
   test("getting the dereferenceable ForwardLinks should result in ratio 0.05660377358490566") {

    val path = getClass.getResource("/rdf.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang)(path)

    val ratio = triples.assessDereferenceableForwardLinks()
    assert(ratio == 0.05660377358490566)
  }
}
