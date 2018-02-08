package net.sansa_stack.rdf.spark.qualityassessment.metrics.relevancy

import org.scalatest.FunSuite
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.jena.riot.Lang
import net.sansa_stack.rdf.spark.io.rdf._
import net.sansa_stack.rdf.spark.qualityassessment._

class AmountOfTriplesTest extends FunSuite with DataFrameSuiteBase {

  test("The amount of triples should match") {
    
    val path = getClass.getResource("/rdf.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang)(path)

    val cnt = triples.assessAmountOfTriples()
    assert(cnt == 0.0)
  }
}