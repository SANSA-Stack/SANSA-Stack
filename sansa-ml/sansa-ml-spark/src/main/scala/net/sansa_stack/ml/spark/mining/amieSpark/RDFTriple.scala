package net.sansa_stack.ml.spark.mining.amieSpark

/**
  * An RDF triple.
  *
  * @author Lorenz Buehmann
  */
case class RDFTriple(subject: String, predicate: String, `object`: String) extends Product3[String, String, String] {
  override def _1: String = subject
  override def _2: String = predicate
  override def _3: String = `object`

  def s: String = subject
  def p: String = predicate
  def o: String = `object`

  override def toString: String = subject + "  " + predicate + "  " + `object`
}
