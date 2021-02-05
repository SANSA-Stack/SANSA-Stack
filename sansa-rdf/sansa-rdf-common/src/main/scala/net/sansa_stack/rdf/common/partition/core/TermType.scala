package net.sansa_stack.rdf.common.partition.core

/**
 * Used for reference to different term types in RDF such that they will be handled consistently
 * across the application.
 *
 * @author Lorenz Buehmann
 */
object TermType {
  /**
   * blank nodes
   */
  val BLANK = 0
  /**
   * IRIs
   */
  val IRI = 1
  /**
   * RDF Literals
   */
  val LITERAL = 2
}
