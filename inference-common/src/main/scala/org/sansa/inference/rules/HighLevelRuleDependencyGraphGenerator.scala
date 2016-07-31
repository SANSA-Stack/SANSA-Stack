package org.sansa.inference.rules

import scala.language.{existentials, implicitConversions}
import scalax.collection.connectivity.GraphComponents

/**
  * A generator for a high-level rule dependency graph for a given rule dependency graph.
  *
  * @author Lorenz Buehmann
  */
object HighLevelRuleDependencyGraphGenerator {

  /**
    * Generates the high-level rule dependency graph for a given rule dependency graph.
    *
    * @param graph the rule dependency graph
    * @return the high-level rule dependency graph
    */
  def generate(graph: RuleDependencyGraph): HighLevelRuleDependencyGraph = {
    // compute the strongly connected components DAG
    val sccDag = GraphComponents.graphToComponents(graph).stronglyConnectedComponentsDag

    // create empty graph
    val g = new HighLevelRuleDependencyGraph(sccDag.nodes.toOuter, sccDag.edges.toOuter)

    g
  }

}
