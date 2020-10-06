package net.sansa_stack.inference.spark.data.model

import org.apache.jena.graph.{Node, Triple}

object TripleUtils extends TripleUtils

/**
  * @author Lorenz Buehmann
  */
class TripleUtils {
  implicit class RichTriple(val triple: Triple) {
    def s: Node = triple.getSubject
    def p: Node = triple.getPredicate
    def o: Node = triple.getObject
  }
}
