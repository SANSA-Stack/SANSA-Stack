package net.sansa_stack.inference.data

/**
  * Defines the types related to the RDF datamodel.
  *
  * This class is inspired by the Banana-RDF project.

  * @author Lorenz Buehmann
  */
trait RDF {

  /**
    * An RDF graph
    */
  type Graph
  /**
    * An RDF triple
    */
  type Triple
  /**
    * An RDF node
    */
  type Node

  type URI <: Node
  /**
    * An RDF blank node
    */
  type BNode <: Node
  /**
    * An RDF literal
    */
  type Literal <: Node
  /**
    * The language tag used in RDF literals
    */
  type Lang

}
