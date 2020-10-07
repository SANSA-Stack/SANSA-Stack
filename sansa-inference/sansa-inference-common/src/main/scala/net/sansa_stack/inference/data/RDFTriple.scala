package net.sansa_stack.inference.data

/**
  * An RDF triple `(s p o)` .
  *
  * @param s the subject
  * @param p the predicate
  * @param o the object
  *
  * @author Lorenz Buehmann
  */
case class RDFTriple(s: String, p: String, o: String) extends Product3[String, String, String] {
  override def _1: String = s
  override def _2: String = p
  override def _3: String = o

  def subject: String = s
  def predicate: String = p
  def `object`: String = o

  override def toString: String = s + "  " + p + "  " + o
}
