package net.sansa_stack.rdf.spark.qualityassessment.dataset

import com.typesafe.config.{ Config, ConfigFactory }
/*
 *
 */
object DatasetUtils {

  @transient lazy val conf: Config = ConfigFactory.load("metrics.conf")
  
  
  val prefixes = conf.getStringList("rdf.qualityassessment.dataset.prefixes")

  /*
   * Subject Class URI
   * @return Class of subjects for which property value is checked.
   */
  val subject: String = conf.getString("rdf.qualityassessment.dataset.subject")
  
  /*
   * Property URI
   * @return Property to be checked.
   */
  val property: String = conf.getString("rdf.qualityassessment.dataset.property")
  
  /*
   * LowerBound
   * Lower bound to evaluate.
   */
  val lowerBound: Double = conf.getDouble("rdf.qualityassessment.dataset.lowerBound")
  
  /*
   * UpperBound
   * Upper bound to evaluate.
   */
  val upperBound: Double = conf.getDouble("rdf.qualityassessment.dataset.upperBound")
}