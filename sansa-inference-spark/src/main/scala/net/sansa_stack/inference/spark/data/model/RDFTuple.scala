package net.sansa_stack.inference.spark.data.model

/**
  * An RDF tuple `(s o)`, i.e. only subject and object are represented.
  *
  * @param s the subject
  * @param o the object
  *
  * @author Lorenz Buehmann
  */
case class RDFTuple(s: String, o: String) extends Product2[String, String] {
    override def _1: String = s
    override def _2: String = o
  }