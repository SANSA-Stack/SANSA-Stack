package net.sansa_stack.inference.data

import org.apache.jena.graph.{Node, Triple}

/**
  * @author Lorenz Buehmann
  */
trait JenaRDFTripleLike extends TripleOps[Jena] {
//  self: Triple =>
//
//  override def s: Node = self.getSubject
//  override def p: Node = self.getPredicate
//  override def o: Node = self.getObject
}
