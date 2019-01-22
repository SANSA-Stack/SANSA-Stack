package net.sansa_stack.ml.common.outliers.vandalismdetection.feature.extraction

import net.sansa_stack.ml.common.outliers.vandalismdetection.feature.Utils._

object Item extends Serializable {

  // 1. from Label Tag
  def getNumberOfLabels(str: String): Integer =
    stringMatchValueAsInt(str, """"value"""" + ":")

  // 2. from description tag
  def getNumberOfDescription(str: String): Integer =
    stringMatchValueAsInt(str, """"value"""" + ":")

  // 3. from Aliases Tag
  def getNumberOfAliases(str: String): Integer =
    stringMatchValueAsInt(str, """"value"""" + ":")

  // 4. from claim tag
  def getNumberOfClaim(str: String): Integer =
    stringMatchValueAsInt(str, """"mainsnak"""" + ":")

  // 5. from Sitelink tag
  def getNumberOfSiteLinks(str: String): Integer =
    stringMatchValueAsInt(str, """"title"""" + ":")

  // 6. from claims tag
  def getNumberOfStatements(str: String): Integer =
    stringMatchValueAsInt(str, """"statement"""")

  // 7. from claims tag
  def getNumberOfReferences(str: String): Integer =
    stringMatchValueAsInt(str, """"references"""" + ":")

  // 8. from claims tag
  def getNumberOfQualifier(str: String): Integer =
    stringMatchValueAsInt(str, """"qualifiers"""" + ":")

  // 9. from claims tag
  def getNumberOfQualifierOrder(str: String): Integer =
    stringMatchValueAsInt(str, """"qualifiers-order"""" + ":")

  // 10. from Sitelink  tag
  def getNumberOfBadges(str: String): Integer =
    stringMatchValueAsInt(str, """"badges"""" + ":")
}
