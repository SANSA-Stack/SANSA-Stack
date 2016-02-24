package org.dissect.rdf.spark.model

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

/**
 * Implicit wrapper for functions for GraphX Graph of Jena Triples.
 *
 * @author Nilesh Chakraborty <nilesh@nileshc.com>
 */
class TripleSparkGraph(graph: SparkGraphX#Graph) extends JenaNodeOps[SparkGraphX] with GraphxGraphOps[SparkGraphX] {
  val sparkContext = graph.edges.sparkContext

  def getTriples: Iterable[SparkGraphX#Triple] =
    getTriples(graph)

  def getSubjects: RDD[SparkGraphX#Node] =
    graph.triplets.map(_.srcAttr)

  def getPredicates: RDD[SparkGraphX#Node] =
    graph.triplets.map(_.attr)

  def getObjects: RDD[SparkGraphX#Node] =
    graph.triplets.map(_.dstAttr)

  def getSubjectsWithPredicate(predicate: SparkGraphX#URI): RDD[SparkGraphX#Node] =
    getSubjectsRDD(graph, predicate)

  def getSubjectsWithPredicate(predicate: SparkGraphX#URI, objectt: SparkGraphX#Node): RDD[SparkGraphX#Node] =
    getSubjectsRDD(graph, predicate, objectt)

  def getObjectsWithPredicate(predicate: SparkGraphX#URI): RDD[SparkGraphX#Node] =
    getObjectsRDD(graph, predicate)

  def getObjectsWithPredicate(subject: SparkGraphX#Node, predicate: SparkGraphX#URI): RDD[SparkGraphX#Node] =
    getObjectsRDD(graph, subject, predicate)

  def mapSubjects(func: (SparkGraphX#Node) => SparkGraphX#Node): SparkGraphX#Graph = {
    // probably expensive - computes RDD of source/subject vertices everytime
    val sourceVertices: VertexRDD[Option] = graph.aggregateMessages[Option](ctx => ctx.sendToSrc(None), (_, _) => None)
    graph.joinVertices(sourceVertices)((_, node, _) => func(node))
  }

  def mapPredicates(func: (SparkGraphX#URI) => SparkGraphX#URI): SparkGraphX#Graph = {
    graph.mapEdges(x => func(x.attr))
  }

  def mapObjects(func: (SparkGraphX#Node) => SparkGraphX#Node): SparkGraphX#Graph = {
    // probably expensive - computes RDD of destination/predicate vertices everytime
    val destinationVertices: VertexRDD[Option] = graph.aggregateMessages[Option](ctx => ctx.sendToSrc(None), (_, _) => None)
    graph.joinVertices(destinationVertices)((_, node, _) => func(node))
  }

  def filterSubjects(func: (SparkGraphX#Node) => Boolean): SparkGraphX#Graph = {
    graph.subgraph({
      triplet => func(triplet.srcAttr)
    }, (_, _) => true)
  }

  def filterPredicates(func: (SparkGraphX#URI) => Boolean): SparkGraphX#Graph = {
    graph.subgraph({
      triplet => func(triplet.attr)
    }, (_, _) => true)
  }

  def filterObjects(func: (SparkGraphX#Node) => Boolean): SparkGraphX#Graph = {
    graph.subgraph({
      triplet => func(triplet.dstAttr)
    }, (_, _) => true)
  }

  def mapURIs(func: (SparkGraphX#URI) => SparkGraphX#URI): SparkGraphX#Graph = {
    def mapper(n: SparkGraphX#Node) = foldNode(n)(func, bnode => bnode, lit => lit)
    graph.mapEdges(x => func(x.attr)).mapVertices{
      case (_: VertexId, node: SparkGraphX#Node) => mapper(node)
    }
  }

  def mapLiterals(func: (SparkGraphX#Literal) => SparkGraphX#Literal): SparkGraphX#Graph = {
    def mapper(n: SparkGraphX#Node) = foldNode(n)(uri => uri, bnode => bnode, func)
    graph.mapVertices{
      case (_: VertexId, node: SparkGraphX#Node) => mapper(node)
    }
  }

  def find(subject: SparkGraphX#NodeMatch, predicate: SparkGraphX#NodeMatch, objectt: SparkGraphX#NodeMatch): SparkGraphX#Graph =
    findGraph(graph, subject, predicate, objectt)
}


