package net.sansa_stack.rdf.spark

import net.sansa_stack.rdf.spark.utils.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.Graph
import org.apache.jena.graph.{ Node, Triple }

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

  }

  /**
   * Adds methods, `astTriple`, `find`, `size` to [[Graph][Node, Node]] that allows to different operations to it.
   */
  implicit class GraphOperations(graph: Graph[Node, Node]) extends Logging {

    /**
     * @see [[GraphOps.toRDD]]
     */
    def toRDD() = GraphOps.toRDD(graph)
    def toDF() = GraphOps.toDF(graph)
    def toDS() = GraphOps.toDS(graph)

    def find(subject: Node, predicate: Node, `object`: Node) = GraphOps.find(graph, subject, predicate, `object`)

    def size() = GraphOps.size(graph)

    def union(other: Graph[Node, Node]) = GraphOps.union(graph, other)
    def difference(other: Graph[Node, Node]) = GraphOps.difference(graph, other)
    def intersection(other: Graph[Node, Node]) = GraphOps.intersection(graph, other)

  }

}