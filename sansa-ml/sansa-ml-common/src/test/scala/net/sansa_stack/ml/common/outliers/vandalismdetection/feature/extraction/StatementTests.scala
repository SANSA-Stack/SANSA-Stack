package net.sansa_stack.ml.common.outliers.vandalismdetection.feature.extraction

import org.scalatest.FunSuite

import net.sansa_stack.ml.common.outliers.vandalismdetection.feature.extraction.Statement._

class StatementTests extends FunSuite {

  val text = """{"type":"item","id":"Q15","labels":{"en":{"language":"en","value":"Africa"}},"descriptions":{"en":{"language":"en","value":"A continent"}},"aliases":[],"claims":[],"sitelinks":[]}"""

  test("getting a value of the item while parsing the statement should match") {
    val itemValue = getItemValue(text)
    assert(itemValue == null)
  }

  test("getting a property while parsing the statement should match") {
    val property = getProperty(text)
    assert(property == null)
  }

  test("getting a data value while parsing the statement should match") {
    val dataValue = getDataValue(text)
    assert(dataValue == null)
  }

}
