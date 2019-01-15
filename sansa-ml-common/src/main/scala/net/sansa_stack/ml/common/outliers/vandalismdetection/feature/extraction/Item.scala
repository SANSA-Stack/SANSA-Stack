package net.sansa_stack.ml.common.outliers.vandalismdetection.feature.extraction

import java.util.regex.{ Matcher, Pattern }

import net.sansa_stack.ml.common.outliers.vandalismdetection.feature.Utils._

object Item extends Serializable {

  // 1. from Label Tag
  def getNumberOfLabels(str: String): Double =
    stringMatchValue(str, """"value"""" + ":")

  // 2. from description tag
  def getNumberOfDescription(str: String): Double =
    stringMatchValue(str, """"value"""" + ":")

  // 3. from Aliases Tag
  def getNumberOfAliases(str: String): Double =
    stringMatchValue(str, """"value"""" + ":")

  // 4. from claim tag
  def getNumberOfClaim(str: String): Double =
    stringMatchValue(str, """"mainsnak"""" + ":")

  // 5. from Sitelink tag
  def getNumberOfSiteLinks(str: String): Double =
    stringMatchValue(str, """"title"""" + ":")

  // 6. from claims tag
  def getNumberOfStatements(str: String): Double =
    stringMatchValue(str, """"statement"""")

  // 7. from claims tag
  def getNumberOfReferences(str: String): Double =
    stringMatchValue(str, """"references"""" + ":")

  // 8. from claims tag
  def getNumberOfQualifier(str: String): Double =
    stringMatchValue(str, """"qualifiers"""" + ":")

  // 9. from claims tag
  def getNumberOfQualifierOrder(str: String): Double =
    stringMatchValue(str, """"qualifiers-order"""" + ":")

  // 10. from Sitelink  tag
  def getNumberOfBadges(str: String): Double =
    stringMatchValue(str, """"badges"""" + ":")
}
