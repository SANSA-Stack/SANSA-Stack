package net.sansa_stack.inference.spark.forwardchaining

import net.sansa_stack.inference.spark.data.model.AbstractRDFGraph
import net.sansa_stack.inference.utils.Logging

/**
  * A forward chaining based reasoner.
  *
  * @author Lorenz Buehmann
  */
abstract class AbstractForwardRuleReasoner[V, G <: AbstractRDFGraph[V, G]] extends Logging {

  /**
    * Applies forward chaining to the given RDF graph and returns a new RDF graph that contains all additional
    * triples based on the underlying set of rules.
    *
    * @param graph the RDF graph
    * @return the materialized RDF graph
    */
  def apply(graph: G) : G
}
