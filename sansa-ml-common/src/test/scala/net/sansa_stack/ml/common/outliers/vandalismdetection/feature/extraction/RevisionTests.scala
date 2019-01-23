package net.sansa_stack.ml.common.outliers.vandalismdetection.feature.extraction

import org.scalatest.FunSuite

import net.sansa_stack.ml.common.outliers.vandalismdetection.feature.extraction.Revision._

class RevisionTests extends FunSuite {

  val text = """{"type":"item","id":"Q15","labels":{"en":{"language":"en","value":"Africa"}},"descriptions":{"en":{"language":"en","value":"A continent"}},"aliases":[],"claims":[],"sitelinks":[]}"""

  val comment = "/* wbsetdescription-set:1|de */ Ein Kontinent"

  test("getting the type of the content should match") {
    val contentType = getContentType(text)
    assert(contentType == "MISC")
  }

  test("checking if the text is latin or not should match") {
    val latin = checkContainLanguageLatinNonLatin(text)
    assert(latin == false)
  }

  test("extracting the language while parsing the revision should match") {
    val lang = extractRevisionLanguage(comment)
    assert(lang == "de")
  }

}
