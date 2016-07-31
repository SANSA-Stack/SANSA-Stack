package org.dissect.inference.rules

import org.apache.jena.reasoner.rulesys.Rule

import scalax.collection.Graph
import scalax.collection.GraphEdge.DiEdge
import scalax.collection.mutable.DefaultGraphImpl

/**
  * Given a set of rules R, a rule dependency graph (RDG) is a directed graph G = (V, E) such that
  * <ol>
  * <li>each vertex in V represents a rule r_i from R and </li>
  * <li>each edge (r_i, r_j) in E denotes the dependency between them </li>
  * </ol>
  *
  * The dependency between two rules r_i and r_j, denoted as r_i -> r_j resp. "r_i depends on r_j"
  * indicates that the result r_j is used as input of r_i. In particular, that means we use the
  * same direction in the graph although one would expect to have an edge from the rule r_j producing the data
  * to the rule r_i consuming the data.
  *
  * @author Lorenz Buehmann
  */
class RuleDependencyGraph(iniNodes: Iterable[Rule] = Set[Rule](),
                          iniEdges: Iterable[DiEdge[Rule]] = Set[DiEdge[Rule]]())
  extends DefaultGraphImpl[Rule, DiEdge](iniNodes, iniEdges)(implicitly, DefaultGraphImpl.defaultConfig){

  def this(graph: Graph[Rule, DiEdge]) = {
    this(graph.nodes.toOuter, graph.edges.toOuter)
  }
  /**
    * @return the set of rules contained in this graph
    */
  def rules() = nodes.toOuter

  def printNodes(): String = rules().map(r => r.getName).mkString("G(", "|",  ")")

  /**
    * Applies topological sort and returns the resulting layers.
    * Each layer contains its level and a set of rules.
    * @return the layers
    */
  def layers() = topologicalSort.right.get.toLayered
    .map(layer => (
      layer._1,
      layer._2.map(node => node.value
      ))
    )

}
