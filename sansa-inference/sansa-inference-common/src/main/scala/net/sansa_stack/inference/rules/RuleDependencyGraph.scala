package net.sansa_stack.inference.rules

import scala.collection.mutable
import scalax.collection.Graph
import scalax.collection.edge.LDiEdge
import scalax.collection.mutable.DefaultGraphImpl

import org.apache.jena.reasoner.rulesys.Rule

/**
  * Given a set of rules R, a rule dependency graph (RDG) is a directed graph
  * G = (V, E) such that
  * <ol>
  *   <li>each vertex in V represents a rule r_i from R and </li>
  *   <li>each edge (r_i, r_j) in E denotes the dependency between them </li>
  * </ol>
  *
  * The dependency between two rules r_i and r_j, denoted as r_i -> r_j,
  * resp. "r_i depends on r_j" indicates that the result of r_j is used as
  * input of r_i. In particular, that means we use the same direction in the
  * graph although one would expect to have an edge from the rule r_j producing
  * the data to the rule r_i consuming the data.
  *
  * Some notes about the types used:
  * The Rule class stems from org.apache.jena.reasoner.rulesys and comprises a
  * list of antecedents (body) and a list of consequents (head), i.e.
  *
  *   consequent [, consequent] <- antecedent [, antecedent]
  *
  * where each consequent or antecedent can be a TriplePattern (i.e. a triple of
  * Nodes, themselves being either variables, wildcards, embedded functors, uri
  * or literal graph nodes), a Functor or a Rule.
  *
  * The Graph and LDiEdge ('labeled directed edge') classes stem from
  * scalax.collection which provides the main graph functionality.
  * Considering a scalax.collection.Graph two kinds of Nodes are distinguished:
  *
  * - Outer Nodes
  *   Outer nodes exist outside of the context of any particular graph and must
  *   be provided by the library user. When added to a graph, they will be
  *   transparently wrapped by a corresponding inner node. Outer nodes must
  *   satisfy the upper bound of the node type parameter of the graph
  * - Inner Nodes
  *   Inner nodes are objects bound to a particular graph. They are
  *   transparently created on graph instantiation or on adding nodes to a
  *   graph. Inner nodes are instances of the inner class NodeT, hence the
  *   term, and are implementing the InnerNodeLike interface. An inner node
  *   acts as a container of the corresponding outer node also providing a
  *   wealth of graph functionality such as diSuccessors or pathTo. Inner nodes
  *   always equal to the contained, user-provided outer node thus facilitating
  *   interchangeability of inner and outer nodes in many situations. Note that
  *   NodeT is a path dependent type such as g.NodeT with g denoting a single
  *   graph instance.
  *
  * (Descriptions taken from http://www.scala-graph.org/guides/core-inner-outer.html)
  *
  * @author Lorenz Buehmann
  * @author Patrick Westphal
  */
class RuleDependencyGraph(iniNodes: Iterable[Rule] = Set[Rule](),
                          iniEdges: Iterable[LDiEdge[Rule]] = Set[LDiEdge[Rule]]())
  extends DefaultGraphImpl[Rule, LDiEdge](iniNodes, iniEdges)(
    implicitly, DefaultGraphImpl.defaultConfig) {

  def this(graph: Graph[Rule, LDiEdge]) = {
    this(graph.nodes.toOuter, graph.edges.toOuter)
  }

  /**
    * This converts the graph-specific inner nodes to its corresponding outer
    * nodes which may exist outside a graph context.
    *
    * @return the set of rules contained in this graph
    */
  def rules(): Set[Rule] = nodes.toOuter

  /**
    * @return a simple string representation of this graph
    */
  def printNodes(): String = rules().map(r => r.getName).mkString("G(", "|", ")")

  /**
    * Applies topological sort and returns the resulting layers.
    * Each layer contains its level and a set of rules.
    *
    * @return the layers
    */
  def layers(): Traversable[(Int, Iterable[Rule])] = topologicalSort.right.get.toLayered
    .map(layer => (
      layer._1,
      layer._2.map(node => node.value
      ))
    )

  /**
    * Returns all nodes that are connected by an edge to itself.
    *
    * @return
    */
  def loopNodes(): mutable.Set[NodeBase] = {
    nodes.filter(n => n.outgoing.map(_.target).contains(n))
  }

  /**
    * Returns true if there is a cycle in the graph, i.e. either
    *
    * - there is a path n1 -> n2 ->  ... -> n1 or
    * - a loop, i.e. an edge that connects a vertex to itself.
    *
    * @return
    */
  def hasCycle(): Boolean = {
    loopNodes().nonEmpty || findCycle.isDefined
  }

}
