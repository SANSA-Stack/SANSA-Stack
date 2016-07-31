package org.dissect.inference.rules

import org.apache.jena.reasoner.rulesys.Rule

import scalax.collection.GraphEdge.DiEdge
import scalax.collection.Graph
import scalax.collection.immutable.DefaultGraphImpl

/**
  * A high-level rule dependency graph denotes a DAG such that
  * (1) each node itself is a rule dependency graph which is strongly connected
  * (2)
  *
  * @author Lorenz Buehmann
  */
/**
  * Given a rule dependency graph (RDG), a high-level rule dependency graph (HLRDG) is a directed acyclic graph G =
  * (V, E)
  * such that
  * <ol>
  * <li>each vertex in V is a rule dependency graph which is strongly connected and </li>
  * <li>each edge (v_i, v_j) in E denotes the dependency between them </li>
  * </ol>
  *
  * The dependency between two rules r_i and r_j, denoted as r_i -> r_j resp. "r_i depends on r_j"
  * indicates that the result r_j is used as input of r_i. In particular, that means we use the
  * same direction in the graph although one would expect to have an edge from the rule r_j producing the data
  * to the rule r_i consuming the data.
  *
  * @author Lorenz Buehmann
  */
class HighLevelRuleDependencyGraph(iniNodes: Iterable[Graph[Rule, DiEdge]] = Set[Graph[Rule, DiEdge]](),
                                   iniEdges: Iterable[DiEdge[Graph[Rule, DiEdge]]] = Set[DiEdge[Graph[Rule, DiEdge]]]())
        extends DefaultGraphImpl[Graph[Rule, DiEdge], DiEdge](iniNodes, iniEdges)(implicitly, DefaultGraphImpl.defaultConfig) {


  /**
    * @return the rule dependency graphs contained in this graphs
    */
  def components() = nodes.toOuter

  /**
    * Applies topological sort and returns the resulting layers.
    * Each layer contains its level and a set of rule dependency graphs.
    * @return the layers
    */
  def layers() = topologicalSort.right.get.toLayered
                  .map(layer => (
                    layer._1,
                    layer._2.map(node => new RuleDependencyGraph(node.value)
                    ))
                  )

  /** Layers of a topological order of a graph or of an isolated graph component.
    * The layers of a topological sort can roughly be defined as follows:
    * a. layer 0 contains all nodes having no predecessors,
    * a. layer n contains those nodes that have only predecessors in anchestor layers
    * with at least one of them contained in layer n - 1
    */
}
