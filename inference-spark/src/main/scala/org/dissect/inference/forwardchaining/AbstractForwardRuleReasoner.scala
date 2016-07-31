package org.dissect.inference.forwardchaining

import org.dissect.inference.data.AbstractRDFGraph

/**
  * A forward chaining based reasoner.
  *
  * @author Lorenz Buehmann
  */
abstract class AbstractForwardRuleReasoner[V, G <: AbstractRDFGraph[V, G]] {

  /**
    * Applies forward chaining to the given RDF graph and returns a new RDF graph that contains all additional
    * triples based on the underlying set of rules.
    *
    * @param graph the RDF graph
    * @return the materialized RDF graph
    */
  def apply(graph: G) : G
}
