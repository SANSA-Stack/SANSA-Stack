package net.sansa_stack.ml.common.outliers.vandalismdetection.feature

import java.util.regex.{ Matcher, Pattern }

object Utils extends Serializable {

  def cleaner(str: String): String = {
    val cleaned_value1 = str.replace("{", "").trim()
    val cleaned_value2 = str.replace("}", "").trim()
    val cleaned_value3 = cleaned_value2.replace("\"", "").trim()
    cleaned_value3.trim()
  }

  def splitBycomma(str: String): Array[String] = {
    val namesList: Array[String] = str.split(",")
    namesList
  }

  def roundDouble(va: Double): Double =
    Math.round(va * 10000).toDouble / 10000

  def ratio(va: Double, median: Double): Double = {
    var tem = va
    if (tem == -1.0) {
      tem = median
    } else {
      tem = va
    }
    tem
  }

  def stringToInt(str: String): Integer =
    str.toInt

  def arrayToString(arra: Array[Double]): String = {
    var tem = ""
    for (i <- 0 until arra.size) {
      if (i == 0) {
        tem = arra(0).toString().trim()
      } else {
        tem = tem + "<1VandalismDetector2>" + arra(i).toString()
      }

    }
    tem.trim()
  }

  def NAstrToDouble(str: String): Double = {
    var tem = 0.0
    if (str == "NA") {
      tem = 0.0
    } else {
      tem = str.toDouble
    }
    tem
  }

  // Character Features: ------ start calculation the Ratio for character:
  def characterRatio(str: String, pattern: Pattern): Double = {
    var charRatio: Double = 0.0;
    if (str != null) {
      val tem: String = pattern.matcher(str).replaceAll("")
      val digits: Double = str.length() - tem.length()
      charRatio = digits / str.length().toDouble
    }
    charRatio
  }

  def extractCharacterRatio(str: String, patternStr: String): Double = {
    val pattern: Pattern = Pattern.compile(patternStr)
    val result: Double = characterRatio(str, pattern)
    result
  }

  def stringMatch(str: String, patternStr: String): Matcher = {
    val pattern: Pattern = Pattern.compile(patternStr)
    val matcher: Matcher = pattern.matcher(str)
    matcher
  }

  def stringMatchValue(str: String, patternStr: String): Double = {
    val matcher = stringMatch(str, patternStr)
    var count: Double = 0.0
    while (matcher.find()) { count += 1; count - 1 }
    count
  }

  def stringMatchValueAsInt(str: String, patternStr: String): Integer = {
    val matcher = stringMatch(str, patternStr)
    var count: Int = 0
    while (matcher.find()) { count += 1; count - 1 }
    count
  }

}
