package net.sansa_stack.ml.common.outliers.vandalismdetection.feature.extraction

import org.scalatest.FunSuite

import net.sansa_stack.ml.common.outliers.vandalismdetection.feature.extraction.Item._

class ItemTests extends FunSuite {

  val text = """{"type":"item","id":"Q15","labels":{"en":{"language":"en","value":"Africa"}},"descriptions":{"en":{"language":"en","value":"A continent"}},"aliases":[],"claims":[],"sitelinks":[]}"""

  test("number of labels ratio should match") {
    val ratio = getNumberOfLabels(text)
    assert(ratio == 2.0)
  }

  test("number of description ratio should match") {
    val ratio = getNumberOfDescription(text)
    assert(ratio == 2.0)
  }

  test("number of aliases ratio should match") {
    val ratio = getNumberOfAliases(text)
    assert(ratio == 2.0)
  }

  test("number of claim ratio should match") {
    val ratio = getNumberOfClaim(text)
    assert(ratio == 0.0)
  }

  test("number of site links ratio should match") {
    val ratio = getNumberOfSiteLinks(text)
    assert(ratio == 0.0)
  }

  test("number of statements ratio should match") {
    val ratio = getNumberOfStatements(text)
    assert(ratio == 0.0)
  }

  test("number of references ratio should match") {
    val ratio = getNumberOfReferences(text)
    assert(ratio == 0.0)
  }

  test("number of qualifier ratio should match") {
    val ratio = getNumberOfQualifier(text)
    assert(ratio == 0.0)
  }

  test("number of qualifier order ratio should match") {
    val ratio = getNumberOfQualifierOrder(text)
    assert(ratio == 0.0)
  }

  test("number of badges ratio should match") {
    val ratio = getNumberOfBadges(text)
    assert(ratio == 0.0)
  }

}
