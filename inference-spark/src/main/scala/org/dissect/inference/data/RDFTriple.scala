package org.dissect.inference.data

/**
  * An RDF triple.
  *
  * @author Lorenz Buehmann
  */
case class RDFTriple(subject: String, predicate: String, `object`: String) extends Product3[String, String, String] {
  override def _1: String = subject
  override def _2: String = predicate
  override def _3: String = `object`

  def s = subject
  def p = predicate
  def o = `object`

  override def toString = subject + "  " + predicate + "  " + `object`
}
