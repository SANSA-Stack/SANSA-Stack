package net.sansa_stack.rdf.spark.model

import org.apache.jena
import org.apache.jena.graph.{Node, Node_URI}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.{Graph => SparkGraph}

import scala.reflect.ClassTag

/**
 * Implicit wrapper for functions for GraphX Graph of Jena Triples.
 *
 * @author Nilesh Chakraborty <nilesh@nileshc.com>
 */
class TripleGraphXGraph(graph: JenaSparkGraphX#Graph) extends JenaSparkGraphXOps {
  val sparkContext = graph.edges.sparkContext

  def getTriples: Iterable[JenaSparkGraphX#Triple] =
     getTriples(graph)

  def getSubjects: RDD[JenaSparkGraphX#Node] =
    graph.triplets.map(_.srcAttr)

  def getPredicates: RDD[JenaSparkGraphX#Node] =
    graph.triplets.map(_.attr)

  def getObjects: RDD[JenaSparkGraphX#Node] =
    graph.triplets.map(_.dstAttr)

  def getSubjectsWithPredicate(predicate: JenaSparkGraphX#URI): RDD[JenaSparkGraphX#Node] =
    getSubjectsRDD(graph, predicate)

  def getSubjectsWithPredicate(predicate: JenaSparkGraphX#URI, objectt: JenaSparkGraphX#Node): RDD[JenaSparkGraphX#Node] =
    getSubjectsRDD(graph, predicate, objectt)

  def getObjectsWithPredicate(predicate: JenaSparkGraphX#URI): RDD[JenaSparkGraphX#Node] =
    getObjectsRDD(graph, predicate)

  def getObjectsWithPredicate(subject: JenaSparkGraphX#Node, predicate: JenaSparkGraphX#URI): RDD[JenaSparkGraphX#Node] =
    getObjectsRDD(graph, subject, predicate)

  def mapSubjects(func: (JenaSparkGraphX#Node) => JenaSparkGraphX#Node): JenaSparkGraphX#Graph = {
    // probably expensive - computes RDD of source/subject vertices everytime
    val sourceVertices: VertexRDD[Option[Int]] = graph.aggregateMessages[Option[Int]](ctx => ctx.sendToSrc(None), (_, _) => None)
    graph.joinVertices(sourceVertices)((_, node, _) => func(node))
  }

  def mapPredicates(func: (JenaSparkGraphX#URI) => JenaSparkGraphX#URI): JenaSparkGraphX#Graph = {
    graph.mapEdges(x => func(x.attr))
  }

  def mapObjects(func: (JenaSparkGraphX#Node) => JenaSparkGraphX#Node): JenaSparkGraphX#Graph = {
    // probably expensive - computes RDD of destination/predicate vertices everytime
    val destinationVertices: VertexRDD[Option[Int]] = graph.aggregateMessages[Option[Int]](ctx => ctx.sendToSrc(None), (_, _) => None)
    graph.joinVertices(destinationVertices)((_, node, _) => func(node))
  }

  def filterSubjects(func: (JenaSparkGraphX#Node) => Boolean): JenaSparkGraphX#Graph = {
    graph.subgraph({
      triplet => func(triplet.srcAttr)
    }, (_, _) => true)
  }

  def filterPredicates(func: (JenaSparkGraphX#URI) => Boolean): JenaSparkGraphX#Graph = {
    graph.subgraph({
      triplet => func(triplet.attr)
    }, (_, _) => true)
  }

  def filterObjects(func: (JenaSparkGraphX#Node) => Boolean): JenaSparkGraphX#Graph = {
    graph.subgraph({
      triplet => func(triplet.dstAttr)
    }, (_, _) => true)
  }

  def mapURIs(func: (JenaSparkGraphX#URI) => JenaSparkGraphX#URI): JenaSparkGraphX#Graph = {
    def mapper(n: JenaSparkGraphX#Node) = foldNode(n)(func, bnode => bnode, lit => lit)
    graph.mapEdges(x => func(x.attr)).mapVertices{
      case (_: VertexId, node: JenaSparkGraphX#Node) => mapper(node)
    }
  }

  def mapLiterals(func: (JenaSparkGraphX#Literal) => JenaSparkGraphX#Literal): JenaSparkGraphX#Graph = {
    def mapper(n: JenaSparkGraphX#Node) = foldNode(n)(uri => uri, bnode => bnode, func)
    graph.mapVertices{
      case (_: VertexId, node: JenaSparkGraphX#Node) => mapper(node)
    }
  }

  def find(subject: JenaSparkGraphX#NodeMatch, predicate: JenaSparkGraphX#NodeMatch, objectt: JenaSparkGraphX#NodeMatch): JenaSparkGraphX#Graph =
    findGraph(graph, subject, predicate, objectt)

  def saveGraphToNTriples(file: String): Unit = saveGraphToNTriples(graph, file)

  def saveGraphToSequenceFile(vertexFile: String, edgeFile: String):Unit = saveGraphToSequenceFile(graph, vertexFile, edgeFile)
}


object TripleGraphXGraph {
  implicit def tripleFunctions(graph: JenaSparkGraphX#Graph): TripleGraphXGraph = new TripleGraphXGraph(graph)
}