package net.sansa_stack.rdf.flink.qualityassessment.dataset

/*
 * Dataset utils.
 */
object DatasetUtils {

  var _prefixes: List[String] = _

  var _subject: String = _
  var _property: String = _

  var _lowerBound: Double = _
  var _upperBound: Double = _

  def setPrefixes(prefixes: List[String]): Unit = {
    _prefixes = prefixes
  }

  def getPrefixes(): List[String] = _prefixes;

  /*
   * Subject Class URI
   * @return Class of subjects for which property value is checked.
   */
  def getSubjectClassURI(): String = _subject

  /*
   * Property URI
   * @return Property to be checked.
   */
  def getPropertyURI(): String = _property

  /*
   * LowerBound
   * Lower bound to evaluate.
   */
  def getLowerBound(): Double = _lowerBound

  /*
   * UpperBound
   * Upper bound to evaluate.
   */
  def getUpperBound(): Double = _upperBound

}
