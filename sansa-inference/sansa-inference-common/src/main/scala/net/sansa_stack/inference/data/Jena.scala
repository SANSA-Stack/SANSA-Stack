package net.sansa_stack.inference.data

import org.apache.jena.graph.{ Graph => JenaGraph, Node => JenaNode, Triple => JenaTriple, _ }

/**
  * Defines the types related to the RDF datamodel in the Apache Jena framework.
  *
  * This class is inspired by the Banana-RDF project.
  *
  * @author Lorenz Buehmann
  */
trait Jena extends RDF {
  type Graph = JenaGraph
  type Triple = JenaTriple
  type Node = JenaNode
  type URI = Node_URI
  type BNode = Node_Blank
  type Literal = Node_Literal
  type Lang = String
}
