package net.sansa_stack.rdf.spark.qualityassessment.metrics.relevancy

import org.scalatest.FunSuite
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.jena.riot.Lang
import net.sansa_stack.rdf.spark.io._
import net.sansa_stack.rdf.spark.qualityassessment._
import java.io.File
import scala.io.Source

class CoverageScopeTest extends FunSuite with DataFrameSuiteBase {
  test("The coverage scope of a dataset should match") {

    val path = getClass.getResource("/rdf.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang)(path)

    val ratio = triples.assessCoverageScope()
    assert(ratio == 0.0)
  }
}