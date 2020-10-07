package net.sansa_stack.ml.spark.mining.amieSpark

/**
 * The SQL schema used for RDF triples in a Dataframe.
 *
 * @author Lorenz Buehmann
 */
object SQLSchema {

  def triplesTable: String = "TRIPLES"

  def subjectCol: String = "subject"

  def predicateCol: String = "predicate"

  def objectCol: String = "object"

}
