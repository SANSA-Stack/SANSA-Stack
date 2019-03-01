package net.sansa_stack.ml.common.outliers.vandalismdetection.feature

import java.util.regex.{ Matcher, Pattern }

import org.scalatest.FunSuite

import net.sansa_stack.ml.common.outliers.vandalismdetection.feature.Utils._

class UtilsTests extends FunSuite {

  test("cleaning the string should match") {
    val str = "id:Q15,labels:{en:{language:en,value:Africa}}"
    val cleanText = cleaner(str)
    assert(cleanText == "id:Q15,labels:{en:{language:en,value:Africa")
  }

  test("splitting the string should match") {
    val str = "id:Q15,labels:{en:{language:en,value:Africa}}"
    val splitText = splitBycomma(str)
    val result = Seq("id:Q15", "labels:{en:{language:en", "value:Africa}}").toArray
    assert(splitText.sameElements(result))
  }

  test("rounding the number should match") {
    assert(roundDouble(3.14159265359) == 3.1416)
  }

  test("converting string to int should match") {
    val str = "15"
    assert(stringToInt(str) == 15)
  }

  test("converting array to string should match") {
    val array = Seq(3.1416, 2.0, 3.0).toArray
    assert(arrayToString(array) == "3.1416<1VandalismDetector2>2.0<1VandalismDetector2>3.0")
  }

  test("ratio of the characters should match") {
    val str = "id:Q15,labels:{en:{language:en,value:Africa}}"
    val pattern = Pattern.compile(":")
    assert(roundDouble(characterRatio(str, pattern)) == 0.1111)
  }

  test("extracting the ratio of the characters should match") {
    val str = "id:Q15,labels:{en:{language:en,value:Africa}}"
    assert(roundDouble(extractCharacterRatio(str, ":")) == 0.1111)
  }

  test("string match should result in 5") {
    val str = "id:Q15,labels:{en:{language:en,value:Africa}}"
    var count = 0
    val metcher = stringMatch(str, ":")
    while (metcher.find()) { count += 1; count - 1 }
    assert(count == 5)
  }

  test("string should match") {
    val str = "id:Q15,labels:{en:{language:en,value:Africa}}"
    assert(stringMatchValue(str, ":") == 5)
  }
}
