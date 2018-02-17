package net.sansa_stack.rdf.spark

import net.sansa_stack.rdf.spark.utils.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.Graph
import org.apache.jena.graph.{ Node, Triple }

/**
 * Wrap up implicit classes/methods o load RDF data into [[GraphX]].
 *
 * @author Gezim Sejdiu
 */

package object graph {

  /**
   * Adds methods, `asGraph` to [[RDD]] that allows to transform as a GraphX representation.
   */
  implicit class GraphLoader(triples: RDD[Triple]) extends Logging {
    /** @see [[net.sansa_stack.rdf.spark.graph.GraphOps.constructGraph]] */
    def asGraph() = GraphOps.constructGraph(triples)

  }

  /**
   * Adds methods, `astTriple`, `find`, `size` to [[Graph][Node, Node]] that allows to different operations to it.
   */
  implicit class GraphOperations(graph: Graph[Node, Node]) extends Logging {

    /** @see [[net.sansa_stack.rdf.spark.graph.GraphOps.toRDD]] */
    def toRDD() = GraphOps.toRDD(graph)
    /** @see [[net.sansa_stack.rdf.spark.graph.GraphOps.toDF]] */
    def toDF() = GraphOps.toDF(graph)
    /** @see [[net.sansa_stack.rdf.spark.graph.GraphOps.toDS]] */
    def toDS() = GraphOps.toDS(graph)

    /** @see [[net.sansa_stack.rdf.spark.graph.GraphOps.find]] */
    def find(subject: Node, predicate: Node, `object`: Node) = GraphOps.find(graph, subject, predicate, `object`)

    /** @see [[net.sansa_stack.rdf.spark.graph.GraphOps.size]] */
    def size() = GraphOps.size(graph)

    /** @see [[net.sansa_stack.rdf.spark.graph.GraphOps.union]] */
    def union(other: Graph[Node, Node]) = GraphOps.union(graph, other)
    /** @see [[net.sansa_stack.rdf.spark.graph.GraphOps.difference]] */
    def difference(other: Graph[Node, Node]) = GraphOps.difference(graph, other)
    /** @see [[net.sansa_stack.rdf.spark.graph.GraphOps.intersection]] */
    def intersection(other: Graph[Node, Node]) = GraphOps.intersection(graph, other)

  }

}