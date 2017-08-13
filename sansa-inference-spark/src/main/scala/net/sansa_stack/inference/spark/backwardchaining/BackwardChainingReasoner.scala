package net.sansa_stack.inference.spark.backwardchaining

import org.apache.jena.reasoner.TriplePattern

/**
  * @author Lorenz Buehmann
  */
trait BackwardChainingReasoner[T] {

  def query(tp: TriplePattern): T

}
