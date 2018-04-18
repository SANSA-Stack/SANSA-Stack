package net.sansa_stack.ml.spark.outliers.vandalismdetection

import java.util.regex.{ Pattern, Matcher }

class ItemFeatures extends Serializable {

  //1.
  def Get_NumberOfLabels(str: String): Double =
    metches(str, """"value"""" + ":")
  //2.
  def Get_NumberOfDescription(str: String): Double =
    metches(str, """"value"""" + ":")
  //3.
  def Get_NumberOfAliases(str: String): Double =
    metches(str, """"value"""" + ":")
  //4.
  def Get_NumberOfClaim(str: String): Double =
    metches(str, """"mainsnak"""" + ":")

  //5.
  def Get_NumberOfSiteLinks(str: String): Double =
    metches(str, """"title"""" + ":")
  //6.
  def Get_NumberOfstatements(str: String): Double =
    metches(str, """"statement"""")
  //7.

  def Get_NumberOfReferences(str: String): Double =
    metches(str, """"references"""" + ":")

  //8.
  def Get_NumberOfQualifier(str: String): Double =
    metches(str, """"qualifiers"""" + ":")

  //9.
  def Get_NumberOfQualifier_Order(str: String): Double =
    metches(str, """"qualifiers-order"""" + ":")

  //10.
  def Get_NumberOfBadges(str: String): Double =
    metches(str, """"badges"""" + ":")

  def metches(inputStr: String, metcher: String) = {
    val pattern: Pattern = Pattern.compile(metcher)
    val matcher: Matcher = pattern.matcher(inputStr)
    var count: Double = 0.0
    while (matcher.find()) { count += 1; count - 1 }
    count
  }

}