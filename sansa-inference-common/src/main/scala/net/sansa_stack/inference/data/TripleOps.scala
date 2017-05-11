package net.sansa_stack.inference.data

/**
  * @author Lorenz Buehmann
  */
trait TripleOps[Rdf <: RDF] {

  /**
    * @return the subject
    */
  def s: Rdf#Node
  /**
    * @return the predicate
    */
  def p: Rdf#URI
  /**
    * @return the object
    */
  def o: Rdf#Node

}
