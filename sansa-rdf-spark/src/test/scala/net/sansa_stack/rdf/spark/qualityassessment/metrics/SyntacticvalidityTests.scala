package net.sansa_stack.rdf.spark.qualityassessment.metrics

import org.scalatest.FunSuite
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.jena.riot.Lang
import net.sansa_stack.rdf.spark.io._
import net.sansa_stack.rdf.spark.qualityassessment._

class SyntacticvalidityTests extends FunSuite with DataFrameSuiteBase {

  test("assessing the literal numeric range checker should result in value 0.0") {

    val path = getClass.getResource("/rdf.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang)(path)

    val value = triples.assessLiteralNumericRangeChecker()
    assert(value == 0.0)
  }

  test("assessing the XSD datatype compatible literals should result in value 80") {

    val path = getClass.getResource("/rdf.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang)(path)

    val value = triples.assessXSDDatatypeCompatibleLiterals()
    assert(value == 80)
  }
}
