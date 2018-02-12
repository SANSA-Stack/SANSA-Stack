package net.sansa_stack.rdf.spark

import net.sansa_stack.rdf.spark.utils.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.Graph
import org.apache.jena.graph.Triple

/**
 * Wrap up implicit classes/methods o load RDF data into [[GraphX]].
 */

package object graph {

  /**
   * Adds methods, `asGraph` to [[RDD]] that allows to transform as a GraphX representation.
   */
  implicit class GraphLoader(triples: RDD[Triple]) extends Logging {

    /**
     * Constructs GraphX graph from RDD of triples
     * @param triples rdd of triples
     * @return object of LoadGraph which contains the constructed  ''graph''.
     */
    def asGraph() = GraphOps.constructGraph(triples)

    /**
     * Computes shortest paths to the given set of landmark vertices, returning a graph where each
     * vertex attribute is a map containing the shortest-path distance to each reachable landmark.
     */
    /*def shortestPaths() =???*/
  }

}