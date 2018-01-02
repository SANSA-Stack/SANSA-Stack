package net.sansa_stack.rdf.spark.qualityassessment.dataset

import com.typesafe.config.{ Config, ConfigFactory }

/*
 *
 */
object DatasetUtils {

  @transient lazy val conf: Config = ConfigFactory.load("metrics.properties")

  val prefixes = conf.getList("rdf.qualityassessment.dataset.prefixes")

  val subject: String = conf.getString("rdf.qualityassessment.dataset.subject")
  val property: String = conf.getString("rdf.qualityassessment.dataset.property")

  val lowerBound: Double = conf.getDouble("rdf.qualityassessment.dataset.lowerBound")
  val upperBound: Double = conf.getDouble("rdf.qualityassessment.dataset.upperBound")

  /*def setPrefixes(prefixes: List[String]) {
    _prefixes = prefixes;

  }
  def getPrefixes() = _prefixes;


   * Subject Class URI
   * @return Class of subjects for which property value is checked.

  def getSubjectClassURI() = _subject


   * Property URI
   * @return Property to be checked.

  def getPropertyURI() = _property


   * LowerBound
   * Lower bound to evaluate.

  def getLowerBound() = _lowerBound


   * UpperBound
   * Upper bound to evaluate.

  def getUpperBound() = _upperBound*/

}