package net.sansa_stack.rdf.spark.qualityassessment.dataset

/*
 * 
 */
object DatasetUtils {

  var _prefixes: List[String] = _;

   def setPrefixes(prefixes: List[String]) {
    _prefixes = prefixes;

  }
  def getPrefixes() = _prefixes;

}