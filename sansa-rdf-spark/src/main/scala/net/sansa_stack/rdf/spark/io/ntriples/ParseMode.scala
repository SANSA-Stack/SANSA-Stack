package net.sansa_stack.rdf.spark.io.ntriples

/**
 * The mode for parsing N-Triples.
 */
object ParseMode extends Enumeration {
  type ParseMode = Value
  val REGEX, SPLIT, JENA = Value
}
