package net.sansa_stack.rdf.flink.model.graph

import org.apache.flink.api.scala.{ DataSet, _ }
import org.apache.flink.graph.{ Edge, Vertex }
import org.apache.flink.graph.scala._
import org.apache.jena.graph.{ Node, NodeFactory, Triple }

/**
 * Flink/Gelly based implementation of DataSet[Triple].
 *
 * @author Gezim Sejdiu
 */
object GraphOps {

  @transient var env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

  /**
   * Constructs Gelly graph from DataSet of triples
   * @param triples DataSet of triples
   * @return object of Graph which contains the constructed  ''graph''.
   */
  def constructGraph(triples: DataSet[Triple]): Graph[Long, Node, Node] = {

    val vertexIDs: DataSet[(Node, Long)] = (triples.map(_.getSubject) union triples.map(_.getObject)).distinct.map(f => (f, f.getURI.toLong)) // indexing

    val vertices: DataSet[(Long, Node)] = vertexIDs.map(x => (x._2, x._1))

    val spo: DataSet[(Node, (Node, Node))] = triples.map(triple => (triple.getSubject, (triple.getPredicate, triple.getObject)))

    val tuples = spo.join(vertexIDs).where(0).equalTo(1).map {
      _ match {
        case ((s, (p, o)), (sv, sid)) => (o, (sid, p))
      }
    }

    val edges = tuples.join(vertexIDs).where(0).equalTo(1).map {
      _ match {
        case ((k, (si, p)), (sv, oi)) => new Edge(si, oi, p)
      }
    }

    val v = vertices.map(f => new Vertex(f._1, f._2))

    Graph.fromDataSet(vertices.map(f => new Vertex(f._1, f._2)), edges, env)
  }

  /**
   * Convert a graph into a DataSet of Triple.
   * @param graph Gelly graph of triples.
   * @return a DataSet of triples.
   */
  def toDataSet(graph: Graph[Long, Node, Node]): DataSet[Triple] = {
    graph.getTriplets() map (x => (Triple.create(x.getSrcVertex.getValue, x.getEdge.getValue, x.getTrgVertex.getValue)))
  }

  /**
   * Get triples  of a given graph.
   * @param graph one instance of the given graph
   * @return [[DataSet[Triple]]] which contains list of the graph triples.
   */
  def getTriples(graph: Graph[Long, Node, Node]): DataSet[Triple] =
    toDataSet(graph)

  /**
   * Get subjects from a given graph.
   * @param graph one instance of the given graph
   * @return [[DataSet[Node]]] which contains list of the subjects.
   */
  def getSubjects(graph: Graph[Long, Node, Node]): DataSet[Node] =
    graph.getTriplets.map(_.getSrcVertex.getValue)

  /**
   * Get predicates from a given graph.
   * @param graph one instance of the given graph
   * @return [[DataSet[Node]]] which contains list of the predicates.
   */
  def getPredicates(graph: Graph[Long, Node, Node]): DataSet[Node] =
    graph.getTriplets.map(_.getEdge.getValue)

  /**
   * Get objects from a given graph.
   * @param graph one instance of the given graph
   * @return [[DataSet[Node]]] which contains list of the objects.
   */
  def getObjects(graph: Graph[Long, Node, Node]): DataSet[Node] =
    graph.getTriplets.map(_.getTrgVertex.getValue)

  /**
   * Compute the size of the graph
   * @param graph
   * @return the number of edges in the graph.
   */
  def size(graph: Graph[Long, Node, Node]): Long =
    graph.getEdges.count()

  /**
   * Return the union of this graph and another one.
   *
   * @param graph of the graph
   * @param other of the other graph
   * @return graph (union of all)
   */
  def union(graph: Graph[Long, Node, Node], other: Graph[Long, Node, Node]): Graph[Long, Node, Node] = {
    Graph.fromDataSet(graph.getVertices.union(other.getVertices.distinct()), graph.getEdges.union(other.getEdges.distinct()), env)
  }
}
