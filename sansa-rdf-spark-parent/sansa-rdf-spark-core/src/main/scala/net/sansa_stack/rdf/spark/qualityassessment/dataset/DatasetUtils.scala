package net.sansa_stack.rdf.spark.qualityassessment.dataset

/*
 * 
 */
object DatasetUtils {

  var _prefixes: List[String] = _;

  var _subject: String = _;
  var _property: String = _;
  def setPrefixes(prefixes: List[String]) {
    _prefixes = prefixes;

  }
  def getPrefixes() = _prefixes;

  /*
   * Subject Class URI 
   * @return Class of subjects for which property value is checked.
   */
  def getSubjectClassURI() = _subject

  /*
   * Property URI 
   * @return Property to be checked.
   */
  def getPropertyURI() = _property

}