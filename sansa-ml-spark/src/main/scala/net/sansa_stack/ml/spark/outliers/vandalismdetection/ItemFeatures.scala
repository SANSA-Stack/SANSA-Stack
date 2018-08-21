package net.sansa_stack.ml.spark.outliers.vandalismdetection

import java.util.regex.{ Matcher, Pattern }

class ItemFeatures extends Serializable {

  // 1.
  def Get_NumberOfLabels(str: String): Double = {

    // from Label Tag
    val input: String = str
    val pattern: Pattern = Pattern.compile(""""value"""" + ":")
    val matcher: Matcher = pattern.matcher(input)
    var count: Double = 0.0
    while (matcher.find()) { count += 1; count - 1 }

    count
  }

  // 2.
  def Get_NumberOfDescription(str: String): Double = {

    // from description tag
    val input: String = str
    val pattern: Pattern = Pattern.compile(""""value"""" + ":")
    val matcher: Matcher = pattern.matcher(input)
    var count: Double = 0.0
    while (matcher.find()) { count += 1; count - 1 }

    count
  }

  // 3.
  def Get_NumberOfAliases(str: String): Double = {

    // from Aliases Tag
    val input: String = str
    val pattern: Pattern = Pattern.compile(""""value"""" + ":")
    val matcher: Matcher = pattern.matcher(input)
    var count: Double = 0.0
    while (matcher.find()) { count += 1; count - 1 }

    count
  }

  // 4.
  def Get_NumberOfClaim(str: String): Double = {

    // from claim tag
    val input: String = str
    val pattern: Pattern = Pattern.compile(""""mainsnak"""" + ":")
    val matcher: Matcher = pattern.matcher(input)
    var count: Double = 0.0
    while (matcher.find()) { count += 1; count - 1 }

    count
  }
  // 5.
  def Get_NumberOfSiteLinks(str: String): Double = {

    // from Sitelink tag
    val input: String = str
    val pattern: Pattern = Pattern.compile(""""title"""" + ":")
    val matcher: Matcher = pattern.matcher(input)
    var count: Double = 0.0
    while (matcher.find()) { count += 1; count - 1 }

    count
  }
  // 6.
  def Get_NumberOfstatements(str: String): Double = {

    // from claims tag
    val input: String = str
    val pattern: Pattern = Pattern.compile(""""statement"""")
    val matcher: Matcher = pattern.matcher(input)
    var count: Double = 0.0
    while (matcher.find()) { count += 1; count - 1 }

    count
  }
  // 7.

  def Get_NumberOfReferences(str: String): Double = {

    // from claims tag
    val input: String = str
    val pattern: Pattern = Pattern.compile(""""references"""" + ":")
    val matcher: Matcher = pattern.matcher(input)
    var count: Double = 0.0
    while (matcher.find()) { count += 1; count - 1 }

    count

    count
  }

  // 8.
  def Get_NumberOfQualifier(str: String): Double = {

    // from claims tag
    val input: String = str
    val pattern: Pattern = Pattern.compile(""""qualifiers"""" + ":")
    val matcher: Matcher = pattern.matcher(input)
    var count: Double = 0.0
    while (matcher.find()) { count += 1; count - 1 }

    count
  }
  // 9.
  def Get_NumberOfQualifier_Order(str: String): Double = {
    // from claims tag
    val input: String = str
    val pattern: Pattern = Pattern.compile(""""qualifiers-order"""" + ":")
    val matcher: Matcher = pattern.matcher(input)
    var count: Double = 0.0
    while (matcher.find()) { count += 1; count - 1 }

    count
  }
  // 10.
  def Get_NumberOfBadges(str: String): Double = {
    // from Sitelink  tag
    val input: String = str
    val pattern: Pattern = Pattern.compile(""""badges"""" + ":")
    val matcher: Matcher = pattern.matcher(input)
    var count: Double = 0.0
    while (matcher.find()) { count += 1; count - 1 }

    count
  }

}
