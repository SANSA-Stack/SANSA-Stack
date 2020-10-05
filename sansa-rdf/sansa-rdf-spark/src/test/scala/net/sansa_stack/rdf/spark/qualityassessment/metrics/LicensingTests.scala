package net.sansa_stack.rdf.spark.qualityassessment.metrics

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import net.sansa_stack.rdf.spark.io._
import net.sansa_stack.rdf.spark.qualityassessment._
import org.apache.jena.riot.Lang
import org.scalatest.FunSuite

class LicensingTests extends FunSuite with DataFrameSuiteBase {

  test("assessing the human readable license should result in value 0.0") {

    val path = getClass.getResource("/rdf.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang)(path)

    val value = triples.assessHumanReadableLicense()
    assert(value == 0.0)
  }

  test("assessing the machine readable license should result in value 0.0") {

    val path = getClass.getResource("/rdf.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang)(path)

    val value = triples.assessMachineReadableLicense()
    assert(value == 0.0)
  }

}
